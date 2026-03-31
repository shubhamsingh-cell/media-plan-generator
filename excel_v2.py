#!/usr/bin/env python3
"""
Consolidated 5-Sheet Excel Generator (v2) for AI Media Plan Generator.

Replaces the 26+ sheet original with 5 focused sheets:
    1. Executive Summary     -- overview, budget, benchmarks, recommendations
    2. Channels & Strategy   -- vetted channels, ad platform analysis, niche boards
    3. Market Intelligence   -- labour market, locations, competition, salary, demand
    4. Sources & Confidence  -- data quality, API status, methodology
    5. ROI Projections       -- per-channel hire forecasts, cost-per-hire, time-to-fill

Design: Sapphire Blue palette, Calibri font throughout, clean professional layout.
All content starts at column B (col A = left margin).

Function signature mirrors generate_excel() -- receives the same enriched data dict
and returns bytes (BytesIO.getvalue()).
"""

from __future__ import annotations

import io
import logging
import re
import datetime
from typing import Any, Dict, List, Optional, Tuple

from openpyxl import Workbook
from openpyxl.styles import (
    Font,
    PatternFill,
    Alignment,
    Border,
    Side,
)
from openpyxl.utils import get_column_letter

from shared_utils import (
    parse_budget,
    INDUSTRY_LABEL_MAP,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Design Tokens -- Sapphire Blue palette
# ---------------------------------------------------------------------------
NAVY = "0F172A"
SAPPHIRE = "2563EB"
BLUE_LIGHT = "DBEAFE"
BLUE_PALE = "EFF6FF"
STONE = "1C1917"
MUTED = "78716C"
WARM_GRAY = "E7E5E4"
OFF_WHITE = "F5F5F4"
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


GREEN = "16A34A"
GREEN_BG = "DCFCE7"
AMBER = "D97706"
AMBER_BG = "FEF3C7"
RED = "DC2626"
RED_BG = "FEE2E2"
WHITE = "FFFFFF"

# ---------------------------------------------------------------------------
# Reusable openpyxl style objects
# ---------------------------------------------------------------------------
_FONT_SECTION = Font(name="Calibri", bold=True, size=14, color=WHITE)
_FONT_SUBSECTION = Font(name="Calibri", bold=True, size=12, color=NAVY)
_FONT_TABLE_HEADER = Font(name="Calibri", bold=True, size=10, color=WHITE)
_FONT_TABLE_HEADER_ALT = Font(name="Calibri", bold=True, size=10, color=NAVY)
_FONT_BODY = Font(name="Calibri", size=10, color=STONE)
_FONT_BODY_BOLD = Font(name="Calibri", bold=True, size=10, color=STONE)
_FONT_FOOTNOTE = Font(name="Calibri", italic=True, size=9, color=MUTED)
_FONT_HERO = Font(name="Calibri", bold=True, size=18, color=NAVY)
_FONT_HERO_VALUE = Font(name="Calibri", bold=True, size=22, color=SAPPHIRE)
_FONT_METRIC_LABEL = Font(name="Calibri", size=9, color=MUTED)
_FONT_METRIC_VALUE = Font(name="Calibri", bold=True, size=14, color=NAVY)
_FONT_GRADE_LARGE = Font(name="Calibri", bold=True, size=36, color=WHITE)

_FILL_NAVY = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
_FILL_SAPPHIRE = PatternFill(
    start_color=SAPPHIRE, end_color=SAPPHIRE, fill_type="solid"
)
_FILL_BLUE_LIGHT = PatternFill(
    start_color=BLUE_LIGHT, end_color=BLUE_LIGHT, fill_type="solid"
)
_FILL_BLUE_PALE = PatternFill(
    start_color=BLUE_PALE, end_color=BLUE_PALE, fill_type="solid"
)
_FILL_OFF_WHITE = PatternFill(
    start_color=OFF_WHITE, end_color=OFF_WHITE, fill_type="solid"
)
_FILL_WHITE = PatternFill(start_color=WHITE, end_color=WHITE, fill_type="solid")
_FILL_GREEN = PatternFill(start_color=GREEN, end_color=GREEN, fill_type="solid")
_FILL_GREEN_BG = PatternFill(
    start_color=GREEN_BG, end_color=GREEN_BG, fill_type="solid"
)
_FILL_AMBER_BG = PatternFill(
    start_color=AMBER_BG, end_color=AMBER_BG, fill_type="solid"
)
_FILL_RED_BG = PatternFill(start_color=RED_BG, end_color=RED_BG, fill_type="solid")

_ALIGN_WRAP = Alignment(wrap_text=True, vertical="top")
_ALIGN_CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
_ALIGN_CENTER_TOP = Alignment(horizontal="center", vertical="top", wrap_text=True)
_ALIGN_LEFT = Alignment(horizontal="left", vertical="top", wrap_text=True)
_ALIGN_RIGHT = Alignment(horizontal="right", vertical="top", wrap_text=True)

_BORDER_THIN = Border(
    left=Side(style="thin", color=WARM_GRAY),
    right=Side(style="thin", color=WARM_GRAY),
    top=Side(style="thin", color=WARM_GRAY),
    bottom=Side(style="thin", color=WARM_GRAY),
)
_BORDER_BOTTOM_SAPPHIRE = Border(bottom=Side(style="medium", color=SAPPHIRE))

# Content column range (B=2 through H=8)
COL_START = 2  # column B
COL_END = 8  # column H
COL_SPAN = COL_END - COL_START + 1  # 7 columns

# ---------------------------------------------------------------------------
# Industry Niche Channels
# ---------------------------------------------------------------------------
INDUSTRY_NICHE_CHANNELS: Dict[str, List[str]] = {
    "healthcare_medical": [
        "Health eCareers",
        "Vivian Health",
        "Nurse.com",
        "PracticeLink",
        "JAMA Career Center",
    ],
    "tech_engineering": [
        "GitHub Jobs",
        "Stack Overflow",
        "Wellfound (AngelList)",
        "Dice",
        "HackerRank",
    ],
    "blue_collar_trades": [
        "TradeHounds",
        "iHire",
        "Jobcase",
        "WorkStep",
        "Skilled Workers Agency",
    ],
    "finance_banking": [
        "eFinancialCareers",
        "Wall Street Oasis",
        "Financial Job Network",
    ],
    "aerospace_defense": [
        "ClearedJobs.Net",
        "ClearanceJobs",
        "Military.com",
        "USAJOBS",
        "Hire Heroes USA",
    ],
    "logistics_supply_chain": [
        "CDLjobs.com",
        "TruckingJobs.com",
        "WarehouseJobs.com",
        "Supply Chain Online",
    ],
    "pharma_biotech": [
        "BioSpace",
        "MedReps",
        "Science Careers (AAAS)",
        "Nature Careers",
    ],
    "retail_consumer": ["RetailGigs", "Snagajob", "Wonolo", "Instawork"],
    "hospitality_travel": ["Hcareers", "Poached", "Culinary Agents", "Harri"],
    "education": ["HigherEdJobs", "SchoolSpring", "K12JobSpot", "Chronicle Vitae"],
    "energy_utilities": ["EnergyJobline", "Rigzone", "Power Magazine Careers"],
    "insurance": ["Insurance Jobs", "The Institutes", "InsuranceJobs.com"],
    "construction_real_estate": [
        "ConstructionJobs.com",
        "iHireConstruction",
        "Built Hire",
    ],
    "automotive": ["AutoJobs.com", "AutomotiveCrossing", "Automotive News Careers"],
    "food_beverage": ["Poached", "Culinary Agents", "RestaurantJobs.com"],
    "media_entertainment": ["MediaBistro", "ProductionHub", "Mandy.com", "Staff Me Up"],
    "telecommunications": [
        "WirelessEstimator",
        "FierceTelecom Jobs",
        "Light Reading Careers",
    ],
    "mental_health": ["Psychology Today Jobs", "SAMHSA Jobs", "APA PsycCareers"],
    "maritime_marine": ["Maritime Jobs", "Rigzone", "Sea Career"],
}


# ---------------------------------------------------------------------------
# Channel Vetting Requirements
# ---------------------------------------------------------------------------
INDUSTRY_CHANNEL_REQUIREMENTS: Dict[str, Dict[str, Any]] = {
    "healthcare_medical": {
        "preferred": [
            "health",
            "nurse",
            "medical",
            "clinical",
            "vivian",
            "doximity",
            "practicelink",
        ],
        "excluded_keywords": ["developer", "github", "stack overflow", "hacker"],
    },
    "tech_engineering": {
        "preferred": [
            "tech",
            "engineer",
            "developer",
            "github",
            "stack overflow",
            "dice",
            "wellfound",
            "hacker",
        ],
        "excluded_keywords": ["nurse", "clinical", "medical staffing"],
    },
    "blue_collar_trades": {
        "preferred": [
            "trade",
            "jobcase",
            "workstep",
            "hourly",
            "skilled",
            "warehouse",
            "cdl",
        ],
        "excluded_keywords": ["executive search", "c-suite"],
    },
    "finance_banking": {
        "preferred": ["finance", "efinancial", "wall street", "banking", "fintech"],
        "excluded_keywords": ["nurse", "clinical", "warehouse"],
    },
    "retail_consumer": {
        "preferred": ["retail", "snagajob", "hourly", "wonolo", "instawork"],
        "excluded_keywords": ["executive search", "c-suite", "clinical"],
    },
    "logistics_supply_chain": {
        "preferred": [
            "logistics",
            "cdl",
            "trucking",
            "warehouse",
            "supply chain",
            "driver",
        ],
        "excluded_keywords": ["clinical", "nurse", "executive search"],
    },
    "hospitality_travel": {
        "preferred": [
            "hospitality",
            "hcareers",
            "poached",
            "culinary",
            "harri",
            "hotel",
        ],
        "excluded_keywords": ["clinical", "developer", "github"],
    },
    "pharma_biotech": {
        "preferred": ["bio", "pharma", "science", "medreps", "clinical research"],
        "excluded_keywords": ["warehouse", "trucking"],
    },
    "aerospace_defense": {
        "preferred": [
            "cleared",
            "clearance",
            "military",
            "defense",
            "usajobs",
            "aerospace",
        ],
        "excluded_keywords": ["retail", "food service"],
    },
    "education": {
        "preferred": [
            "education",
            "highered",
            "schoolspring",
            "k12",
            "academic",
            "teaching",
        ],
        "excluded_keywords": ["warehouse", "trucking", "clinical"],
    },
}

ROLE_CHANNEL_REQUIREMENTS: Dict[str, Dict[str, Any]] = {
    "executive": {
        "preferred": [
            "executive search",
            "linkedin",
            "c-suite",
            "board",
            "spencer stuart",
        ],
        "excluded_keywords": ["hourly", "snagajob", "warehouse", "entry level"],
    },
    "professional": {
        "preferred": ["linkedin", "indeed", "glassdoor", "professional"],
        "excluded_keywords": [],
    },
    "hourly": {
        "preferred": ["snagajob", "wonolo", "instawork", "jobcase", "hourly", "shift"],
        "excluded_keywords": ["executive search", "c-suite", "spencer stuart"],
    },
    "clinical": {
        "preferred": ["vivian", "nurse", "health", "medical", "clinical", "doximity"],
        "excluded_keywords": ["warehouse", "trucking", "developer"],
    },
    "trades": {
        "preferred": ["trade", "ihire", "skilled", "construction", "cdl"],
        "excluded_keywords": ["executive search", "c-suite"],
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def _safe_num(val: Any, default: float = 0.0) -> float:
    """Safely convert a value to float."""
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        clean = val.replace(",", "").replace("$", "").replace("%", "").strip()
        try:
            return float(clean)
        except (ValueError, TypeError):
            return default
    return default


def _fmt_currency(val: Any, prefix: str = "$", show_cents: bool = False) -> str:
    """Format a numeric value as currency."""
    num = _safe_num(val)
    if num == 0:
        return f"{prefix}0"
    if abs(num) >= 1_000_000:
        return f"{prefix}{num / 1_000_000:,.1f}M"
    if abs(num) >= 10_000 and not show_cents:
        return f"{prefix}{num:,.0f}"
    if show_cents or abs(num) < 10:
        return f"{prefix}{num:,.2f}"
    return f"{prefix}{num:,.0f}"


def _fmt_number(val: Any, decimals: int = 0) -> str:
    """Format a number with thousand separators."""
    num = _safe_num(val)
    if num == 0:
        return "0"
    if decimals > 0:
        return f"{num:,.{decimals}f}"
    return f"{num:,.0f}"


def _fmt_pct(val: Any, decimals: int = 1) -> str:
    """Format as percentage. If val < 1, treat as fraction (0.05 -> 5.0%)."""
    num = _safe_num(val)
    if num == 0:
        return "0%"
    # If value looks like a fraction (less than 1 but not negative), convert
    if 0 < num < 1:
        num *= 100
    return f"{num:.{decimals}f}%"


def _flatten_value(val: Any, max_depth: int = 3) -> str:
    """Safely flatten a nested dict/list into a readable string.

    CRITICAL: Never call str() on raw nested structures. This iterates
    through dicts and lists to produce human-readable key-value text.
    """
    if val is None:
        return ""
    if isinstance(val, str):
        return val
    if isinstance(val, bool):
        return "Yes" if val else "No"
    if isinstance(val, (int, float)):
        return str(val)

    if max_depth <= 0:
        return "[nested data]"

    if isinstance(val, list):
        flat_items = []
        for item in val[:10]:  # cap at 10 items
            flat_items.append(_flatten_value(item, max_depth - 1))
        return ", ".join(flat_items)

    if isinstance(val, dict):
        parts = []
        for k, v in list(val.items())[:10]:
            flat_v = _flatten_value(v, max_depth - 1)
            if flat_v:
                parts.append(f"{k}: {flat_v}")
        return "; ".join(parts)

    return str(val)[:200]


def _get_roles(data: dict) -> List[str]:
    """Extract normalized role strings from data dict."""
    roles_raw = data.get("target_roles") or data.get("roles") or []
    if isinstance(roles_raw, str):
        return [r.strip() for r in roles_raw.split(",") if r.strip()]
    roles = []
    for r in roles_raw:
        if isinstance(r, dict):
            roles.append(r.get("title") or r.get("role") or str(r))
        elif isinstance(r, str):
            roles.append(r.strip())
    return roles or ["General"]


def _get_locations(data: dict) -> List[str]:
    """Extract location strings from data dict."""
    locs = data.get("locations") or []
    if isinstance(locs, str):
        return [locs]
    if isinstance(locs, list):
        return [str(loc) for loc in locs if loc] or ["United States"]
    return ["United States"]


def _get_budget_numeric(data: dict) -> float:
    """Parse budget from data dict to numeric value."""
    budget_raw = data.get("budget") or data.get("budget_range") or ""
    return parse_budget(budget_raw, default=100_000.0) if budget_raw else 100_000.0


def _get_industry_label(industry_key: str) -> str:
    """Convert industry key to display label."""
    return INDUSTRY_LABEL_MAP.get(industry_key, industry_key.replace("_", " ").title())


def _grade_from_score(score: float) -> str:
    """Convert a 0-1 confidence score to a letter grade."""
    if score >= 0.9:
        return "A"
    if score >= 0.8:
        return "B"
    if score >= 0.65:
        return "C"
    if score >= 0.5:
        return "D"
    return "F"


def _grade_fill(grade: str) -> PatternFill:
    """Return fill color for a confidence grade."""
    if grade in ("A", "B"):
        return _FILL_GREEN_BG
    if grade == "C":
        return _FILL_AMBER_BG
    return _FILL_RED_BG


def _grade_font(grade: str) -> Font:
    """Return font color for a confidence grade."""
    if grade in ("A", "B"):
        return Font(name="Calibri", bold=True, size=10, color=GREEN)
    if grade == "C":
        return Font(name="Calibri", bold=True, size=10, color=AMBER)
    return Font(name="Calibri", bold=True, size=10, color=RED)


def _fit_fill(fit: str) -> PatternFill:
    """Return fill for channel fit rating."""
    fit_lower = fit.lower() if isinstance(fit, str) else ""
    if "excellent" in fit_lower:
        return _FILL_GREEN_BG
    if "good" in fit_lower:
        return _FILL_BLUE_PALE
    return _FILL_AMBER_BG


def _fit_score_fill(score: float) -> PatternFill:
    """Return fill for numeric fit score."""
    if score >= 0.7:
        return _FILL_GREEN_BG
    if score >= 0.4:
        return _FILL_AMBER_BG
    return _FILL_RED_BG


def _fit_score_font(score: float) -> Font:
    """Return font for numeric fit score."""
    if score >= 0.7:
        return Font(name="Calibri", bold=True, size=10, color=GREEN)
    if score >= 0.4:
        return Font(name="Calibri", bold=True, size=10, color=AMBER)
    return Font(name="Calibri", bold=True, size=10, color=RED)


def _detect_role_type(roles: List[str]) -> str:
    """Detect dominant role type from role titles."""
    if not roles:
        return "professional"
    combined = " ".join(r.lower() for r in roles)
    if any(
        kw in combined
        for kw in [
            "nurse",
            "physician",
            "clinical",
            "medical",
            "therapist",
            "rn ",
            "lpn",
            "cna",
        ]
    ):
        return "clinical"
    if any(
        kw in combined
        for kw in ["ceo", "cfo", "vp ", "director", "chief", "president", "executive"]
    ):
        return "executive"
    if any(
        kw in combined
        for kw in [
            "warehouse",
            "driver",
            "assembler",
            "operator",
            "laborer",
            "mechanic",
            "technician",
            "welder",
        ]
    ):
        return "hourly"
    if any(
        kw in combined
        for kw in ["plumber", "electrician", "carpenter", "hvac", "mason", "welder"]
    ):
        return "trades"
    return "professional"


# ═══════════════════════════════════════════════════════════════════════════════
# WORKSHEET HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def _set_column_widths(ws, widths: Dict[int, float]):
    """Set column widths. Key = 1-based column number."""
    for col_num, width in widths.items():
        ws.column_dimensions[get_column_letter(col_num)].width = width


def _write_section_header(ws, row: int, title: str) -> int:
    """Write a full-width section header (navy background, white text).
    Returns the row AFTER the header (header row + 1).
    """
    ws.merge_cells(
        start_row=row,
        start_column=COL_START,
        end_row=row,
        end_column=COL_END,
    )
    cell = ws.cell(row=row, column=COL_START, value=title.upper())
    cell.font = _FONT_SECTION
    cell.fill = _FILL_NAVY
    cell.alignment = _ALIGN_LEFT
    # Set row height for header prominence
    ws.row_dimensions[row].height = 28
    return row + 1


def _write_subsection_header(ws, row: int, title: str) -> int:
    """Write a sub-section header (navy text, sapphire bottom border).
    Returns the row AFTER the header (header row + 1).
    """
    ws.merge_cells(
        start_row=row,
        start_column=COL_START,
        end_row=row,
        end_column=COL_END,
    )
    cell = ws.cell(row=row, column=COL_START, value=title)
    cell.font = _FONT_SUBSECTION
    cell.alignment = _ALIGN_LEFT
    cell.border = _BORDER_BOTTOM_SAPPHIRE
    ws.row_dimensions[row].height = 22
    return row + 1


def _write_table_header(
    ws,
    row: int,
    headers: List[str],
    col_start: int = COL_START,
    fill: PatternFill = None,
) -> int:
    """Write a table header row. Returns the next row."""
    use_fill = fill or _FILL_SAPPHIRE
    use_font = _FONT_TABLE_HEADER if fill is None else _FONT_TABLE_HEADER_ALT
    if fill == _FILL_BLUE_LIGHT:
        use_font = _FONT_TABLE_HEADER_ALT
    for i, header in enumerate(headers):
        cell = ws.cell(row=row, column=col_start + i, value=header)
        cell.font = use_font
        cell.fill = use_fill
        cell.alignment = _ALIGN_CENTER
        cell.border = _BORDER_THIN
    ws.row_dimensions[row].height = 22
    return row + 1


def _write_table_row(
    ws,
    row: int,
    values: List[Any],
    col_start: int = COL_START,
    alternate: bool = False,
    fonts: List[Optional[Font]] = None,
    fills: List[Optional[PatternFill]] = None,
    aligns: List[Optional[Alignment]] = None,
) -> int:
    """Write a single table data row. Returns the next row."""
    row_fill = _FILL_BLUE_PALE if alternate else _FILL_WHITE
    for i, val in enumerate(values):
        cell = ws.cell(row=row, column=col_start + i, value=val)
        cell.font = fonts[i] if fonts and i < len(fonts) and fonts[i] else _FONT_BODY
        cell.fill = fills[i] if fills and i < len(fills) and fills[i] else row_fill
        cell.alignment = (
            aligns[i] if aligns and i < len(aligns) and aligns[i] else _ALIGN_WRAP
        )
        cell.border = _BORDER_THIN
    return row + 1


def _write_metric_card(ws, row: int, col: int, label: str, value: str):
    """Write a metric card (label above, value below) in a 2-row, 2-col block."""
    # Value cell
    cell_val = ws.cell(row=row, column=col, value=value)
    cell_val.font = _FONT_METRIC_VALUE
    cell_val.fill = _FILL_OFF_WHITE
    cell_val.alignment = _ALIGN_CENTER
    cell_val.border = _BORDER_THIN
    # Merge value across 2 columns if space allows
    if col + 1 <= COL_END:
        ws.merge_cells(start_row=row, start_column=col, end_row=row, end_column=col + 1)
    # Label cell (row below)
    cell_lbl = ws.cell(row=row + 1, column=col, value=label)
    cell_lbl.font = _FONT_METRIC_LABEL
    cell_lbl.fill = _FILL_OFF_WHITE
    cell_lbl.alignment = _ALIGN_CENTER
    cell_lbl.border = _BORDER_THIN
    if col + 1 <= COL_END:
        ws.merge_cells(
            start_row=row + 1, start_column=col, end_row=row + 1, end_column=col + 1
        )


def _write_kv_row(
    ws, row: int, key: str, value: str, col_start: int = COL_START
) -> int:
    """Write a key-value pair spanning 2 + 5 columns. Returns next row."""
    # Key cell (B:C)
    ws.merge_cells(
        start_row=row, start_column=col_start, end_row=row, end_column=col_start + 1
    )
    cell_k = ws.cell(row=row, column=col_start, value=key)
    cell_k.font = _FONT_BODY_BOLD
    cell_k.alignment = _ALIGN_LEFT
    cell_k.border = _BORDER_THIN
    cell_k.fill = _FILL_OFF_WHITE
    # Value cell (D:H)
    ws.merge_cells(
        start_row=row, start_column=col_start + 2, end_row=row, end_column=COL_END
    )
    cell_v = ws.cell(row=row, column=col_start + 2, value=value)
    cell_v.font = _FONT_BODY
    cell_v.alignment = _ALIGN_WRAP
    cell_v.border = _BORDER_THIN
    return row + 1


def _write_footnote(ws, row: int, text: str) -> int:
    """Write a footnote row spanning full width. Returns next row."""
    ws.merge_cells(
        start_row=row,
        start_column=COL_START,
        end_row=row,
        end_column=COL_END,
    )
    cell = ws.cell(row=row, column=COL_START, value=text)
    cell.font = _FONT_FOOTNOTE
    cell.alignment = _ALIGN_LEFT
    return row + 1


def _write_attribution_footer(ws, row: int) -> int:
    """Write data attribution footer. Returns next row."""
    row = _write_footnote(
        ws,
        row,
        f"Generated by Nova AI Media Plan Generator | {datetime.date.today().strftime('%B %d, %Y')} | "
        "Data from BLS, Census, SEC EDGAR, O*NET, Google Ads, Meta, LinkedIn, and 15+ additional APIs",
    )
    return row


# ═══════════════════════════════════════════════════════════════════════════════
# CHANNEL VETTING FUNCTION
# ═══════════════════════════════════════════════════════════════════════════════


def vet_channels(
    channels: List[Dict[str, Any]],
    industry: str,
    roles: List[str],
    locations: List[str],
) -> List[Dict[str, Any]]:
    """Vet and score channels against industry, role, and location requirements.

    Args:
        channels: List of channel dicts (from _channels_db or budget allocations).
                  Each must have at least a 'name' key.
        industry: Industry key (e.g. "healthcare_medical").
        roles: List of role title strings.
        locations: List of location strings.

    Returns:
        Sorted list of channel dicts with added 'fit' and 'fit_score' keys.
        Sorted by fit_score descending (Excellent > Good > Fair).
    """
    if not channels:
        return []

    ind_reqs = INDUSTRY_CHANNEL_REQUIREMENTS.get(industry, {})
    ind_preferred = [kw.lower() for kw in ind_reqs.get("preferred") or []]
    ind_excluded = [kw.lower() for kw in ind_reqs.get("excluded_keywords") or []]

    role_type = _detect_role_type(roles)
    role_reqs = ROLE_CHANNEL_REQUIREMENTS.get(role_type, {})
    role_preferred = [kw.lower() for kw in role_reqs.get("preferred") or []]
    role_excluded = [kw.lower() for kw in role_reqs.get("excluded_keywords") or []]

    # Deduplicate by normalized name
    seen_names = set()
    deduped = []
    for ch in channels:
        name = ch.get("name") or "" if isinstance(ch, dict) else str(ch)
        # Strip common suffixes BEFORE removing non-alphanumeric chars
        norm_name = name.lower().strip()
        for suffix in [".com", ".net", ".org", ".io", " jobs", " job"]:
            norm_name = norm_name.replace(suffix, "")
        norm_name = re.sub(r"[^a-z0-9]", "", norm_name)
        if norm_name and norm_name not in seen_names:
            seen_names.add(norm_name)
            if isinstance(ch, dict):
                deduped.append(ch)
            else:
                deduped.append({"name": str(ch)})

    # Detect if locations are US-only
    location_lower = " ".join(loc.lower() for loc in locations)
    _us_states = {
        "california",
        "new york",
        "texas",
        "florida",
        "illinois",
        "ohio",
        "georgia",
        "michigan",
        "pennsylvania",
        "virginia",
        "washington",
        "arizona",
        "massachusetts",
        "colorado",
        "minnesota",
        "oregon",
        "nevada",
        "tennessee",
        "indiana",
        "north carolina",
        "south carolina",
        "new jersey",
        "maryland",
        "missouri",
        "wisconsin",
        "connecticut",
        "iowa",
        "utah",
        "kansas",
        "kentucky",
        "louisiana",
        "alabama",
        "oklahoma",
        "nebraska",
        "mississippi",
        "arkansas",
        "montana",
        "new mexico",
        "new hampshire",
        "idaho",
        "hawaii",
        "maine",
        "rhode island",
        "delaware",
        "south dakota",
        "north dakota",
        "alaska",
        "vermont",
        "wyoming",
        "west virginia",
    }

    def _is_us_location(loc_str: str) -> bool:
        ll = loc_str.lower().strip()
        if "united states" in ll or ll == "usa" or ll == "us":
            return True
        # Check against state names (exact or in comma-separated parts)
        parts = [p.strip().lower() for p in ll.split(",")]
        return any(p in _us_states for p in parts)

    is_us_only = all(_is_us_location(loc) for loc in locations) if locations else True

    vetted = []
    for ch in deduped:
        name = ch.get("name") or ""
        name_lower = name.lower()

        # Check exclusions -- remove if channel matches industry OR role exclusions
        excluded = False
        for excl_kw in ind_excluded + role_excluded:
            if excl_kw and excl_kw in name_lower:
                excluded = True
                break
        if excluded:
            continue

        # Check geographic fit -- skip international-only boards if US-only
        intl_only_keywords = ["apac", "emea", "latam"]
        if is_us_only and any(kw in name_lower for kw in intl_only_keywords):
            continue

        # Score the channel -- start with a category-based baseline
        # so different channel types get differentiated scores even without
        # exact keyword matches.
        cat = _roi_category_for_channel(name)
        _category_baselines: Dict[str, float] = {
            "niche_board": 0.75,
            "referral": 0.80,
            "career_site": 0.70,
            "events": 0.65,
            "staffing": 0.65,
            "job_board": 0.60,
            "social": 0.55,
            "programmatic": 0.50,
            "search": 0.55,
            "display": 0.45,
            "email": 0.55,
            "employer_branding": 0.60,
            "regional": 0.60,
        }
        score = _category_baselines.get(cat, 0.50)

        # Industry preference match
        for pref in ind_preferred:
            if pref in name_lower:
                score += 0.20
                break

        # Role preference match
        for pref in role_preferred:
            if pref in name_lower:
                score += 0.10
                break

        # Major boards always get a baseline boost (broad fit)
        major_boards = [
            "indeed",
            "linkedin",
            "glassdoor",
            "ziprecruiter",
            "google",
            "meta",
            "facebook",
        ]
        if any(mb in name_lower for mb in major_boards):
            score = max(score, 0.65)

        # Niche board for the industry = excellent
        niche_for_industry = INDUSTRY_NICHE_CHANNELS.get(industry, [])
        if any(
            niche.lower() in name_lower or name_lower in niche.lower()
            for niche in niche_for_industry
        ):
            score = max(score, 0.85)

        # Industry-specific channel type bonus: niche boards score higher
        # for matching industries (e.g., healthcare niche boards for healthcare)
        if cat == "niche_board" and ind_preferred:
            score = max(score, 0.80)

        # Determine fit label
        if score >= 0.8:
            fit = "Excellent"
        elif score >= 0.6:
            fit = "Good"
        else:
            fit = "Fair"

        ch_copy = dict(ch)
        ch_copy["fit"] = fit
        ch_copy["fit_score"] = round(min(score, 1.0), 2)
        vetted.append(ch_copy)

    # Sort by fit_score descending
    vetted.sort(key=lambda x: x.get("fit_score") or 0, reverse=True)
    return vetted


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE CONFIDENCE / BIAS ASSESSMENT
# ═══════════════════════════════════════════════════════════════════════════════


def assess_source_bias(source_name: str) -> Dict[str, Any]:
    """Categorize a data source and assess potential bias.

    Returns dict with:
        category: str -- the source category
        confidence_modifier: float -- multiplier for confidence (0.6 - 1.0)
        bias: str -- bias assessment text
    """
    if not source_name or not isinstance(source_name, str):
        return {
            "category": "Unknown",
            "confidence_modifier": 0.60,
            "bias": "Unknown - Unable to verify independence",
        }

    name = source_name.lower().strip()

    # Government / Academic (highest trust)
    gov_academic_keywords = [
        "bls",
        "bureau of labor",
        "census",
        "acs",
        "o*net",
        "onet",
        "careeronestop",
        "fred",
        "imf",
        "world bank",
        "worldbank",
        "sec",
        "edgar",
        "usda",
        "nih",
        "cdc",
        "shrm",
        "university",
        "academic",
        "aaas",
        "government",
        "federal",
        "datausa",
        "geonames",
        "rest countries",
        "restcountries",
    ]
    if any(kw in name for kw in gov_academic_keywords):
        return {
            "category": "Government / Academic",
            "confidence_modifier": 1.0,
            "bias": "Low - Independent",
        }

    # Industry Analyst
    analyst_keywords = [
        "gartner",
        "forrester",
        "deloitte",
        "mckinsey",
        "bain",
        "bcg",
        "korn ferry",
        "mercer",
        "aon",
        "pwc",
        "ernst & young",
        "ey ",
        "kpmg",
        "accenture",
    ]
    if any(kw in name for kw in analyst_keywords):
        return {
            "category": "Industry Analyst",
            "confidence_modifier": 0.95,
            "bias": "Low - Independent analyst",
        }

    # Platform / Publisher (may promote their own platform)
    platform_keywords = [
        "indeed",
        "linkedin",
        "glassdoor",
        "ziprecruiter",
        "monster",
        "careerbuilder",
        "google ads",
        "google trends",
        "meta ads",
        "facebook ads",
        "bing ads",
        "tiktok ads",
        "snap ads",
        "clearbit",
        "wikipedia",
        "teleport",
    ]
    if any(kw in name for kw in platform_keywords):
        return {
            "category": "Platform / Publisher",
            "confidence_modifier": 0.75,
            "bias": "Medium - May promote own platform",
        }

    # Vendor / Marketer
    vendor_keywords = [
        "appcast",
        "recruitics",
        "icims",
        "phenom",
        "radancy",
        "pandologic",
        "talroo",
        "programmatic",
        "vendor",
        "marketer",
        "recruitology",
        "nexxt",
        "jovian",
    ]
    if any(kw in name for kw in vendor_keywords):
        return {
            "category": "Vendor / Marketer",
            "confidence_modifier": 0.70,
            "bias": "Medium-High - Promotes own services",
        }

    # Internal / First-Party (Joveo)
    internal_keywords = ["joveo", "mojo", "first-party", "internal", "campaign data"]
    if any(kw in name for kw in internal_keywords):
        return {
            "category": "Internal / First-Party",
            "confidence_modifier": 0.85,
            "bias": "Low - First-party campaign data",
        }

    # Unknown
    return {
        "category": "Unknown",
        "confidence_modifier": 0.60,
        "bias": "Unknown - Unable to verify independence",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SHEET 1: EXECUTIVE SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════


def _build_sheet_executive_summary(
    ws, data: dict, research_mod=None, load_kb_fn=None, classify_tier_fn=None
):
    """Build Sheet 1: Executive Summary."""
    ws.title = "Executive Summary"
    ws.sheet_properties.tabColor = SAPPHIRE

    # Column widths
    _set_column_widths(
        ws,
        {
            1: 3,  # A: margin
            2: 18,  # B
            3: 18,  # C
            4: 18,  # D
            5: 18,  # E
            6: 18,  # F
            7: 18,  # G
            8: 18,  # H
            9: 14,  # I: CPA column (budget allocation table extends to 9 cols)
            10: 14,  # J: ROI Score column
        },
    )

    client_name = data.get("client_name", "Client")
    industry = data.get("industry", "general_entry_level")
    industry_label = _get_industry_label(industry)
    locations = _get_locations(data)
    roles = _get_roles(data)
    budget_num = _get_budget_numeric(data)
    duration = data.get("campaign_duration", "Not specified")
    hire_volume = data.get("hire_volume") or ""
    work_env = data.get("work_environment", "hybrid")

    budget_alloc = data.get("_budget_allocation", {})
    total_proj = budget_alloc.get("total_projected", {})
    sufficiency = budget_alloc.get("sufficiency", {})
    channel_allocs = budget_alloc.get("channel_allocations", {})
    warnings = budget_alloc.get("warnings") or []
    recommendations = budget_alloc.get("recommendations") or []

    synthesized = data.get("_synthesized", {})
    enriched = data.get("_enriched", {})
    tier_groups = data.get("_tier_groups", {})

    row = 2

    # ── 1. Campaign Overview ──
    # Hero banner
    ws.merge_cells(
        start_row=row, start_column=COL_START, end_row=row, end_column=COL_END
    )
    hero_cell = ws.cell(
        row=row, column=COL_START, value=f"Recruitment Media Plan: {client_name}"
    )
    hero_cell.font = _FONT_HERO
    hero_cell.alignment = _ALIGN_LEFT
    ws.row_dimensions[row].height = 36
    row += 1

    # Subtitle
    ws.merge_cells(
        start_row=row, start_column=COL_START, end_row=row, end_column=COL_END
    )
    sub_cell = ws.cell(
        row=row,
        column=COL_START,
        value=f"{industry_label} | {work_env.title()} | "
        f"Generated {datetime.date.today().strftime('%B %d, %Y')}",
    )
    sub_cell.font = _FONT_FOOTNOTE
    sub_cell.alignment = _ALIGN_LEFT
    row += 2

    row = _write_section_header(ws, row, "Campaign Overview")

    # 2x3 metric cards grid
    metrics = [
        ("Budget", _fmt_currency(budget_num)),
        ("Duration", str(duration)),
        ("Locations", str(len(locations))),
        ("Roles", str(len(roles))),
        ("Industry", industry_label),
        ("Hire Volume", str(hire_volume) if hire_volume else "TBD"),
    ]
    card_row = row
    for idx, (label, value) in enumerate(metrics):
        col_offset = (idx % 3) * 2  # 0, 2, 4
        col = COL_START + col_offset
        _write_metric_card(ws, card_row + (idx // 3) * 3, col, label, value)
    row = card_row + 6  # 2 rows of cards * 3 height each
    row += 1  # gap

    # ── 2. Company Intelligence ──
    company_intel = {}
    if research_mod:
        try:
            company_intel = research_mod.get_company_intelligence(client_name)
        except Exception as exc:
            logger.warning("Company intelligence lookup failed: %s", exc)

    # Also pull from synthesized competitive intelligence
    comp_intel = synthesized.get("competitive_intelligence", {})
    company_profile = comp_intel.get("company_profile", {})

    if company_intel.get("matched") or company_profile:
        row = _write_section_header(ws, row, "Company Intelligence")

        # Merge company_intel and company_profile, preferring company_intel
        display_fields = [
            (
                "Industry",
                company_intel.get(
                    "industry", company_profile.get("industry", industry_label)
                ),
            ),
            ("Size", company_intel.get("size", company_profile.get("size") or "")),
            (
                "Employer Brand",
                company_intel.get(
                    "employer_brand", company_profile.get("employer_brand") or ""
                ),
            ),
            (
                "Hiring Channels",
                company_intel.get(
                    "hiring_channels", company_profile.get("hiring_channels") or ""
                ),
            ),
            (
                "Known Strategies",
                company_intel.get(
                    "known_strategies", company_profile.get("known_strategies") or ""
                ),
            ),
            (
                "Glassdoor Rating",
                company_intel.get(
                    "glassdoor_rating", company_profile.get("glassdoor_rating") or ""
                ),
            ),
            (
                "Talent Focus",
                company_intel.get(
                    "talent_focus", company_profile.get("talent_focus") or ""
                ),
            ),
        ]
        for key, val in display_fields:
            val_str = _flatten_value(val) if val else ""
            if val_str:
                row = _write_kv_row(ws, row, key, val_str)
        row += 1

    # ── 3. Budget Allocation ──
    row = _write_section_header(ws, row, "Budget Allocation")

    # Hero metrics row: Total Budget | Projected Hires | Cost/Hire
    hero_metrics = [
        ("Total Budget", _fmt_currency(budget_num)),
        ("Projected Hires", _fmt_number(total_proj.get("hires") or 0)),
        ("Cost / Hire", _fmt_currency(total_proj.get("cost_per_hire") or 0)),
    ]
    for idx, (label, value) in enumerate(hero_metrics):
        col = COL_START + idx * 2
        _write_metric_card(ws, row, col, label, value)
    row += 3  # 2-row cards + gap

    # Sufficiency grade
    grade_str = sufficiency.get("grade") or ""
    grade_msg = sufficiency.get(
        "message", sufficiency.get("budget_reality_check", {}).get("message") or ""
    )
    if grade_str:
        ws.merge_cells(
            start_row=row, start_column=COL_START, end_row=row, end_column=COL_END
        )
        cell = ws.cell(
            row=row,
            column=COL_START,
            value=f"Budget Sufficiency: {grade_str} -- {grade_msg}",
        )
        cell.font = _FONT_BODY_BOLD
        cell.alignment = _ALIGN_LEFT
        if "A" in str(grade_str) or "B" in str(grade_str):
            cell.fill = _FILL_GREEN_BG
        elif "C" in str(grade_str):
            cell.fill = _FILL_AMBER_BG
        else:
            cell.fill = _FILL_RED_BG
        row += 1

    row += 1

    # Channel allocation table
    if channel_allocs:
        headers = [
            "Channel",
            "%",
            "Amount",
            "Proj. Clicks",
            "Proj. Apps",
            "Proj. Hires",
            "CPC",
            "CPA",
            "ROI Score",
        ]
        row_h = row
        for i, h in enumerate(headers):
            cell = ws.cell(row=row_h, column=COL_START + i, value=h)
            cell.font = _FONT_TABLE_HEADER
            cell.fill = _FILL_SAPPHIRE
            cell.alignment = _ALIGN_CENTER
            cell.border = _BORDER_THIN
        ws.row_dimensions[row_h].height = 22
        row = row_h + 1

        sorted_channels = sorted(
            channel_allocs.items(),
            key=lambda x: x[1].get("dollar_amount", x[1].get("dollars") or 0),
            reverse=True,
        )
        _row_idx = 0
        for ch_name, ch_data in sorted_channels[:15]:
            # Bug 23: Skip garbage rows where all metrics are zero/empty
            _ch_cpc = ch_data.get("cpc") or 0
            _ch_cpa = ch_data.get("cpa") or 0
            _ch_dollars = ch_data.get("dollar_amount", ch_data.get("dollars") or 0) or 0
            _ch_roi = ch_data.get("roi_score") or 0
            _ch_pct = ch_data.get("percentage") or 0
            if not any([_ch_cpc, _ch_cpa, _ch_dollars, _ch_roi, _ch_pct]):
                continue
            idx = _row_idx
            _row_idx += 1
            _display_name = ch_name.replace("_", " ").title()
            values = [
                _display_name,
                f"{_safe_num(_ch_pct):.1f}%",
                _fmt_currency(_ch_dollars),
                _fmt_number(ch_data.get("projected_clicks") or 0),
                _fmt_number(ch_data.get("projected_applications") or 0),
                _fmt_number(ch_data.get("projected_hires") or 0),
                _fmt_currency(_ch_cpc, show_cents=True),
                _fmt_currency(_ch_cpa, show_cents=True),
                str(_ch_roi or ""),
            ]
            for i, val in enumerate(values):
                cell = ws.cell(row=row, column=COL_START + i, value=val)
                cell.font = _FONT_BODY
                cell.fill = _FILL_BLUE_PALE if idx % 2 else _FILL_WHITE
                cell.alignment = _ALIGN_CENTER if i > 0 else _ALIGN_LEFT
                cell.border = _BORDER_THIN
            row += 1
    row += 1

    # ── 4. Recruitment Benchmarks ──
    # Load benchmarks from knowledge base (NOT hardcoded)
    kb_benchmarks = {}
    if load_kb_fn:
        try:
            kb = load_kb_fn()
            kb_benchmarks = kb.get("recruitment_benchmarks", {}).get(
                "industry_benchmarks", {}
            )
            if not kb_benchmarks:
                # Try alternative paths
                kb_benchmarks = kb.get("benchmarks", {})
        except Exception as exc:
            logger.warning("Knowledge base load failed for benchmarks: %s", exc)

    if kb_benchmarks:
        row = _write_section_header(ws, row, "Recruitment Benchmarks")

        # Determine client's relevant region(s) from locations
        def _detect_region(loc: str) -> str:
            loc_lower = loc.lower()
            us_indicators = [
                "united states",
                "usa",
                "california",
                "new york",
                "texas",
                "florida",
                "chicago",
                "los angeles",
                "houston",
                "phoenix",
            ]
            if any(kw in loc_lower for kw in us_indicators):
                return "North America"
            eu_indicators = [
                "uk",
                "united kingdom",
                "germany",
                "france",
                "spain",
                "italy",
                "netherlands",
                "europe",
            ]
            if any(kw in loc_lower for kw in eu_indicators):
                return "Europe"
            apac_indicators = [
                "india",
                "china",
                "japan",
                "singapore",
                "australia",
                "asia",
                "pacific",
            ]
            if any(kw in loc_lower for kw in apac_indicators):
                return "APAC"
            latam_indicators = [
                "brazil",
                "mexico",
                "colombia",
                "argentina",
                "latin america",
            ]
            if any(kw in loc_lower for kw in latam_indicators):
                return "LATAM"
            return "North America"  # default

        client_regions = list(set(_detect_region(loc) for loc in locations))

        # Try to find industry-specific benchmarks
        ind_bench = kb_benchmarks.get(
            industry, kb_benchmarks.get("general_entry_level", {})
        )
        if isinstance(ind_bench, dict):
            row = _write_subsection_header(
                ws, row, f"Industry Benchmarks: {industry_label}"
            )

            # If benchmarks have regional breakdown, filter to client regions
            regional = ind_bench.get("regional", ind_bench.get("by_region", {}))
            if regional and isinstance(regional, dict):
                filtered_regional = {
                    k: v
                    for k, v in regional.items()
                    if any(r.lower() in k.lower() for r in client_regions)
                }
                if filtered_regional:
                    headers = ["Region", "CPA", "CPC", "Cost/Hire", "Apply Rate"]
                    row = _write_table_header(ws, row, headers)
                    for idx, (region, rdata) in enumerate(filtered_regional.items()):
                        if isinstance(rdata, dict):
                            values = [
                                region,
                                _flatten_value(
                                    rdata.get(
                                        "cpa", rdata.get("cost_per_application") or ""
                                    )
                                ),
                                _flatten_value(
                                    rdata.get("cpc", rdata.get("cost_per_click") or "")
                                ),
                                _flatten_value(
                                    rdata.get("cph", rdata.get("cost_per_hire") or "")
                                ),
                                _flatten_value(rdata.get("apply_rate") or ""),
                            ]
                        else:
                            values = [region, _flatten_value(rdata), "", "", ""]
                        row = _write_table_row(ws, row, values, alternate=idx % 2 == 1)
            else:
                # Flat benchmarks (no regional breakdown)
                for key, val in ind_bench.items():
                    if key not in ("regional", "by_region", "metadata"):
                        val_str = _flatten_value(val)
                        if val_str:
                            row = _write_kv_row(
                                ws, row, key.replace("_", " ").title(), val_str
                            )
        row += 1

    # ── 5. Executive Strategic Narrative (LLM-generated) ──
    # Generate a C-suite quality narrative using Claude Haiku via the LLM router directly
    # (avoids circular import with app.py)
    exec_narrative = ""
    try:
        from llm_router import LLMRouter, TASK_CAMPAIGN_PLAN

        _exec_router = LLMRouter()
        _narrative_prompt = (
            f"Write a 4-5 sentence executive summary for a recruitment media plan.\n\n"
            f"Client: {client_name}\n"
            f"Industry: {industry_label}\n"
            f"Budget: {_fmt_currency(budget_num)}\n"
            f"Locations: {', '.join(str(l) for l in locations[:5])}\n"
            f"Roles: {', '.join(str(r) for r in roles[:5])}\n"
            f"Hire Volume: {hire_volume}\n"
            f"Duration: {duration}\n"
            f"Projected Hires: {total_proj.get('hires') or 'TBD'}\n"
            f"Cost/Hire: {_fmt_currency(total_proj.get('cost_per_hire') or 0)}\n"
            f"Budget Grade: {sufficiency.get('grade') or 'N/A'}\n"
            f"Top Channels: {', '.join(list(channel_allocs.keys())[:5])}\n\n"
            f"Write as a senior recruitment strategist presenting to a VP of Talent Acquisition. "
            f"Include: (1) market thesis -- why this plan will succeed, "
            f"(2) ROI projection summary with specific numbers, "
            f"(3) key risks to monitor, "
            f"(4) recommended next steps with timeline. "
            f"Be specific, cite data from above, no generic statements."
        )
        _exec_result = _exec_router.call_llm(
            messages=[{"role": "user", "content": _narrative_prompt}],
            system_prompt=(
                "You are a senior recruitment marketing strategist presenting to "
                "C-suite executives. Write with authority, cite specific data points, "
                "and explain causal reasoning. Every sentence must contain a number "
                "or specific insight. No fluff, no platitudes."
            ),
            task_type=TASK_CAMPAIGN_PLAN,
            max_tokens=600,
        )
        exec_narrative = _exec_result.get("text") or ""
    except ImportError:
        logger.warning("LLM router not available for executive narrative")
    except Exception as exc:
        logger.warning("Executive narrative generation failed (non-fatal): %s", exc)

    if exec_narrative:
        row = _write_section_header(ws, row, "Executive Strategic Summary")
        # Wrap the narrative in a merged cell
        ws.merge_cells(
            start_row=row, start_column=COL_START, end_row=row + 3, end_column=COL_END
        )
        cell = ws.cell(row=row, column=COL_START, value=exec_narrative)
        cell.font = Font(name="Calibri", size=11, color=NAVY)
        cell.alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)
        cell.fill = _FILL_BLUE_PALE
        for r in range(row, row + 4):
            ws.row_dimensions[r].height = 20
        row += 5

    # ── 5b. Risk Analysis ──
    row = _write_section_header(ws, row, "Risk Analysis")
    risk_items: list[tuple[str, str, str]] = []  # (risk, impact, mitigation)

    # Budget risk
    proj_hires = total_proj.get("hires") or 0
    cph = total_proj.get("cost_per_hire") or 0
    if proj_hires > 0 and cph > 0:
        hires_at_20_pct_increase = int(budget_num / (cph * 1.2)) if cph > 0 else 0
        risk_items.append(
            (
                "Budget Risk: CPA Inflation",
                f"If CPA rises 20%, projected hires drop from {proj_hires:,.0f} to {hires_at_20_pct_increase:,.0f} "
                f"({proj_hires - hires_at_20_pct_increase:,.0f} fewer hires)",
                "Build 10-15% budget contingency; diversify to lower-CPA channels",
            )
        )

    # Market timing risk
    import datetime as _dt_risk

    current_month = _dt_risk.date.today().month
    _q2_months = {4, 5, 6}
    _q1_months = {1, 2, 3}
    campaign_start = data.get("campaign_start_month") or current_month
    if isinstance(campaign_start, int) and campaign_start in _q2_months:
        risk_items.append(
            (
                "Market Timing: Q2 Competition",
                "Q2 hiring is 15-20% more competitive than Q4 due to fiscal year budget cycles",
                "Front-load spend in first 4 weeks; lock in niche channel inventory early",
            )
        )
    elif isinstance(campaign_start, int) and campaign_start in _q1_months:
        risk_items.append(
            (
                "Market Timing: New Year Surge",
                "Q1 sees 25% increase in job seeker activity but also 20% more employer competition",
                "Leverage higher candidate supply with aggressive apply-rate optimization",
            )
        )

    # Channel dependency risk
    if channel_allocs:
        sorted_ch = sorted(
            channel_allocs.items(),
            key=lambda x: x[1].get("percentage", 0),
            reverse=True,
        )
        top_2_pct = sum(ch[1].get("percentage", 0) for ch in sorted_ch[:2])
        if top_2_pct > 55:
            ch_names = ", ".join(ch[0] for ch in sorted_ch[:2])
            risk_items.append(
                (
                    "Channel Dependency",
                    f"{top_2_pct:.0f}% of budget concentrated on {ch_names} -- "
                    f"single-channel disruption could impact {top_2_pct * proj_hires / 100:.0f} projected hires",
                    "Diversify to 4+ channels; maintain 3 backup channels on standby",
                )
            )

    # Competitive pressure risk
    gold_standard_data = data.get("_gold_standard") or {}
    competitor_map = gold_standard_data.get("competitor_mapping") or {}
    n_competitive_cities = sum(
        1
        for city_key, info in competitor_map.items()
        if not str(city_key).startswith("_")
        and str(info.get("hiring_intensity") or "").lower() in ("high", "very_high")
    )
    if n_competitive_cities > 0:
        risk_items.append(
            (
                "Competitive Pressure",
                f"{n_competitive_cities} market(s) have high competitive intensity -- "
                f"Fortune 500+ companies actively hiring similar roles",
                "Differentiate with employer brand messaging; emphasize career growth, culture, flexibility",
            )
        )

    if risk_items:
        headers = ["Risk Factor", "Impact Assessment", "Mitigation Strategy"]
        _risk_fill = PatternFill(start_color=RED, end_color=RED, fill_type="solid")
        row_h = row
        for i, h in enumerate(headers):
            col_start = COL_START + i * 2
            ws.merge_cells(
                start_row=row_h,
                start_column=col_start,
                end_row=row_h,
                end_column=col_start + 1,
            )
            cell = ws.cell(row=row_h, column=col_start, value=h)
            cell.font = _FONT_TABLE_HEADER
            cell.fill = _risk_fill
            cell.alignment = _ALIGN_CENTER
            cell.border = _BORDER_THIN
        ws.row_dimensions[row_h].height = 22
        row = row_h + 1

        for idx, (risk, impact, mitigation) in enumerate(risk_items):
            bg_fill = _FILL_RED_BG if idx % 2 == 0 else _FILL_WHITE
            for col_idx, val in enumerate([risk, impact, mitigation]):
                col_start = COL_START + col_idx * 2
                ws.merge_cells(
                    start_row=row,
                    start_column=col_start,
                    end_row=row,
                    end_column=col_start + 1,
                )
                cell = ws.cell(row=row, column=col_start, value=val)
                cell.font = _FONT_BODY if col_idx > 0 else _FONT_BODY_BOLD
                cell.fill = bg_fill
                cell.alignment = _ALIGN_WRAP
                cell.border = _BORDER_THIN
            ws.row_dimensions[row].height = 40
            row += 1
    else:
        ws.merge_cells(
            start_row=row, start_column=COL_START, end_row=row, end_column=COL_END
        )
        ws.cell(
            row=row,
            column=COL_START,
            value="Insufficient data to generate risk analysis. Add locations and budget for detailed risk assessment.",
        ).font = _FONT_FOOTNOTE
        row += 1

    row += 1

    # ── 6. Key Recommendations ──
    all_recommendations = list(recommendations)

    # Add industry-specific recommendations from tier groups
    if tier_groups:
        for tier_name, tier_data in tier_groups.items():
            tier_info = tier_data.get("tier_info", {})
            tier_roles = tier_data.get("roles") or []
            strategy = tier_info.get("sourcing_strategy") or ""
            if strategy and tier_roles:
                all_recommendations.append(
                    f"{tier_name} roles ({', '.join(tier_roles[:3])}): {strategy}"
                )

    if all_recommendations or warnings:
        row = _write_section_header(ws, row, "Key Recommendations")

        if warnings:
            row = _write_subsection_header(ws, row, "Warnings")
            for w in warnings[:5]:
                ws.merge_cells(
                    start_row=row,
                    start_column=COL_START,
                    end_row=row,
                    end_column=COL_END,
                )
                cell = ws.cell(row=row, column=COL_START, value=f"  {w}")
                cell.font = Font(name="Calibri", size=10, color=RED)
                cell.fill = _FILL_RED_BG
                cell.alignment = _ALIGN_WRAP
                row += 1
            row += 1

        if all_recommendations:
            row = _write_subsection_header(ws, row, "Recommendations")
            for idx, rec in enumerate(all_recommendations[:8]):
                ws.merge_cells(
                    start_row=row,
                    start_column=COL_START,
                    end_row=row,
                    end_column=COL_END,
                )
                cell = ws.cell(row=row, column=COL_START, value=f"  {idx + 1}. {rec}")
                cell.font = _FONT_BODY
                cell.alignment = _ALIGN_WRAP
                cell.fill = _FILL_BLUE_PALE if idx % 2 else _FILL_WHITE
                row += 1

    row += 2
    _write_attribution_footer(ws, row)


# ═══════════════════════════════════════════════════════════════════════════════
# SHEET 2: CHANNELS & CHANNEL STRATEGY
# ═══════════════════════════════════════════════════════════════════════════════


def _build_sheet_channels(ws, data: dict, research_mod=None, load_kb_fn=None):
    """Build Sheet 2: Channels & Channel Strategy."""
    ws.title = "Channels & Strategy"
    ws.sheet_properties.tabColor = SAPPHIRE

    _set_column_widths(
        ws,
        {
            1: 3,
            2: 22,
            3: 14,
            4: 14,
            5: 14,
            6: 14,
            7: 14,
            8: 18,
        },
    )

    industry = data.get("industry", "general_entry_level")
    industry_label = _get_industry_label(industry)
    roles = _get_roles(data)
    locations = _get_locations(data)

    budget_alloc = data.get("_budget_allocation", {})
    channel_allocs = budget_alloc.get("channel_allocations", {})
    synthesized = data.get("_synthesized", {})
    channels_db = data.get("_channels_db", {})
    collar_type = data.get("_collar_type", "mixed")

    row = 2

    # ── 1. Channel Strategy Overview ──
    row = _write_section_header(ws, row, "Channel Strategy Overview")

    if channel_allocs:
        # Top channels sorted by budget allocation
        sorted_channels = sorted(
            channel_allocs.items(),
            key=lambda x: x[1].get("dollar_amount", x[1].get("dollars") or 0),
            reverse=True,
        )

        headers = [
            "Channel",
            "Budget %",
            "Amount",
            "Category",
            "CPC",
            "Confidence",
            "Fit",
        ]
        row = _write_table_header(ws, row, headers)

        for idx, (ch_name, ch_data) in enumerate(sorted_channels[:15]):
            roi = ch_data.get("roi_score") or ""
            confidence = ch_data.get("confidence", "medium")
            category = ch_data.get("category") or ""

            values = [
                ch_name,
                f"{_safe_num(ch_data.get('percentage') or 0):.1f}%",
                _fmt_currency(
                    ch_data.get("dollar_amount", ch_data.get("dollars") or 0)
                ),
                category.replace("_", " ").title() if category else "",
                _fmt_currency(ch_data.get("cpc") or 0, show_cents=True),
                confidence.title() if isinstance(confidence, str) else str(confidence),
                roi if isinstance(roi, str) else str(roi),
            ]
            row = _write_table_row(ws, row, values, alternate=idx % 2 == 1)
    else:
        ws.merge_cells(
            start_row=row, start_column=COL_START, end_row=row, end_column=COL_END
        )
        ws.cell(
            row=row, column=COL_START, value="No channel allocation data available."
        ).font = _FONT_BODY
        row += 1

    row += 2

    # ── 2. Recommended Channels (Vetted) ──
    row = _write_section_header(ws, row, "Recommended Channels (Vetted)")

    # Build channel list from multiple sources
    raw_channels = []

    # From channel allocations
    for ch_name, ch_data in channel_allocs.items():
        raw_channels.append(
            {
                "name": ch_name,
                "category": ch_data.get("category") or "",
                "budget_pct": ch_data.get("percentage") or 0,
                "cpc": ch_data.get("cpc") or 0,
            }
        )

    # From channels DB (add channels not already in allocations)
    alloc_names_lower = {n.lower() for n in channel_allocs.keys()}
    if isinstance(channels_db, dict):
        for cat_key in [
            "traditional",
            "non_traditional",
            "programmatic",
            "social_media",
            "niche",
            "regional",
        ]:
            cat_channels = channels_db.get(cat_key, {})
            if isinstance(cat_channels, dict):
                for ch_name, ch_info in cat_channels.items():
                    if ch_name.lower() not in alloc_names_lower:
                        entry = {"name": ch_name, "category": cat_key}
                        if isinstance(ch_info, dict):
                            entry.update(ch_info)
                        raw_channels.append(entry)
            elif isinstance(cat_channels, list):
                for ch_item in cat_channels:
                    ch_name = (
                        ch_item.get("name", str(ch_item))
                        if isinstance(ch_item, dict)
                        else str(ch_item)
                    )
                    if ch_name.lower() not in alloc_names_lower:
                        entry = {"name": ch_name, "category": cat_key}
                        if isinstance(ch_item, dict):
                            entry.update(ch_item)
                        raw_channels.append(entry)

    # Vet the channels
    vetted = vet_channels(raw_channels, industry, roles, locations)

    if vetted:
        headers = [
            "Channel",
            "Category",
            "Fit",
            "CPC",
            "Budget %",
            "Strategic Rationale",
            "Fit Score",
        ]
        row = _write_table_header(ws, row, headers)

        for idx, ch in enumerate(vetted[:20]):  # cap at 20
            fit = ch.get("fit", "Fair")
            fit_score = ch.get("fit_score", 0.5)
            ch_name = ch.get("name") or ""
            ch_category = (ch.get("category") or "").replace("_", " ").title()
            ch_cpc = ch.get("cpc") or 0
            ch_pct = ch.get("budget_pct") or 0
            notes = ch.get("description", ch.get("notes") or "")
            if isinstance(notes, dict):
                notes = _flatten_value(notes)

            # Build strategic rationale with WHY reasoning
            rationale_parts: list[str] = []
            if fit == "Strong" and fit_score >= 0.7:
                rationale_parts.append(
                    f"High-fit ({fit_score:.0%}) for {industry_label}"
                )
            elif fit == "Good":
                rationale_parts.append(f"Good industry alignment ({fit_score:.0%})")
            if ch_cpc > 0:
                rationale_parts.append(f"CPC ${ch_cpc:.2f}")
            if ch_pct > 15:
                rationale_parts.append(
                    f"Primary channel -- {ch_pct:.0f}% of budget for volume"
                )
            elif ch_pct > 5:
                rationale_parts.append(f"Supporting channel at {ch_pct:.0f}%")
            # Add role/location context
            if roles and len(roles) <= 3:
                rationale_parts.append(f"targets {', '.join(roles[:2])}")
            if locations and len(locations) <= 3:
                rationale_parts.append(
                    f"in {', '.join(str(l).split(',')[0] for l in locations[:2])}"
                )
            if notes and len(notes) > 10:
                rationale_parts.append(notes[:60])

            rationale = (
                "; ".join(rationale_parts)
                if rationale_parts
                else (notes[:80] if notes else "")
            )

            values = [
                ch_name,
                ch_category,
                fit,
                (_fmt_currency(ch_cpc, show_cents=True) if ch_cpc else ""),
                (f"{_safe_num(ch_pct):.1f}%" if ch_pct else ""),
                rationale[:120],
                f"{fit_score:.2f}",
            ]

            # Custom fills for fit column
            fit_fills = [
                None,
                None,
                _fit_fill(fit),
                None,
                None,
                None,
                _fit_score_fill(fit_score),
            ]
            fit_fonts = [None, None, None, None, None, None, _fit_score_font(fit_score)]
            row = _write_table_row(
                ws,
                row,
                values,
                alternate=idx % 2 == 1,
                fills=fit_fills,
                fonts=fit_fonts,
            )
    else:
        ws.merge_cells(
            start_row=row, start_column=COL_START, end_row=row, end_column=COL_END
        )
        ws.cell(
            row=row, column=COL_START, value="No vetted channels available."
        ).font = _FONT_BODY
        row += 1

    row += 2

    # ── 3. Ad Platform Analysis ──
    ad_platforms = synthesized.get("ad_platform_analysis", {})

    row = _write_section_header(ws, row, "Ad Platform Analysis")

    if ad_platforms:
        # Build headers dynamically -- exclude ROI Projection
        # Include Audience Reach only if at least one platform has non-zero reach
        has_reach = any(
            _safe_num(p.get("audience_reach", p.get("estimated_reach") or 0)) > 0
            for p in ad_platforms.values()
            if isinstance(p, dict)
        )

        headers = ["Platform", "CPC", "CPM", "CPA", "Fit Score"]
        if has_reach:
            headers.insert(4, "Audience Reach")

        row = _write_table_header(ws, row, headers)

        _plat_idx = 0
        for plat_key, plat_data in ad_platforms.items():
            if not isinstance(plat_data, dict):
                continue
            plat_name = plat_data.get(
                "platform_name", plat_key.replace("_", " ").title()
            )
            fit_score = _safe_num(plat_data.get("fit_score") or 0)
            _p_cpc = plat_data.get("avg_cpc", plat_data.get("cpc") or 0) or 0
            _p_cpm = plat_data.get("avg_cpm", plat_data.get("cpm") or 0) or 0
            _p_cpa = plat_data.get("avg_cpa", plat_data.get("cpa") or 0) or 0
            if not any([_p_cpc, _p_cpm, _p_cpa, fit_score]):
                continue
            idx = _plat_idx
            _plat_idx += 1

            values = [
                plat_name,
                _fmt_currency(
                    plat_data.get("avg_cpc", plat_data.get("cpc") or 0), show_cents=True
                ),
                _fmt_currency(
                    plat_data.get("avg_cpm", plat_data.get("cpm") or 0), show_cents=True
                ),
                _fmt_currency(
                    plat_data.get("avg_cpa", plat_data.get("cpa") or 0), show_cents=True
                ),
            ]

            if has_reach:
                reach = _safe_num(
                    plat_data.get(
                        "audience_reach", plat_data.get("estimated_reach") or 0
                    )
                )
                values.append(_fmt_number(reach) if reach > 0 else "")

            values.append(f"{fit_score:.2f}")

            # Color-code fit scores
            fit_col_idx = len(values) - 1
            row_fills = [None] * len(values)
            row_fonts = [None] * len(values)
            row_fills[fit_col_idx] = _fit_score_fill(fit_score)
            row_fonts[fit_col_idx] = _fit_score_font(fit_score)

            row = _write_table_row(
                ws,
                row,
                values,
                alternate=idx % 2 == 1,
                fills=row_fills,
                fonts=row_fonts,
            )
    else:
        # Fallback: show a "data pending" note with general guidance
        ws.merge_cells(
            start_row=row, start_column=COL_START, end_row=row, end_column=COL_END
        )
        ws.cell(
            row=row,
            column=COL_START,
            value=(
                "Ad platform performance data pending -- live API integration "
                "with Google Ads, Meta, and LinkedIn will populate this section. "
                "In the interim, refer to the Channel Benchmarks table on the "
                "Executive Summary sheet for estimated CPC/CPA ranges."
            ),
        ).font = _FONT_FOOTNOTE
        ws.cell(row=row, column=COL_START).alignment = _ALIGN_WRAP
        row += 1

    row += 2

    # ── 4. Industry Niche Channels ──
    niche_channels = INDUSTRY_NICHE_CHANNELS.get(industry, [])

    if niche_channels:
        row = _write_section_header(ws, row, f"Niche Channels: {industry_label}")

        headers = ["Channel", "Type", "Relevance"]
        row = _write_table_header(ws, row, headers)

        for idx, ch_name in enumerate(niche_channels):
            values = [
                ch_name,
                "Industry Niche Board",
                "High - Specialized for " + industry_label,
            ]
            row = _write_table_row(ws, row, values, alternate=idx % 2 == 1)

    row += 2
    _write_attribution_footer(ws, row)


# ═══════════════════════════════════════════════════════════════════════════════
# SHEET 3: MARKET INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════════════════


def _build_sheet_market_intelligence(ws, data: dict, research_mod=None):
    """Build Sheet 3: Market Intelligence."""
    ws.title = "Market Intelligence"
    ws.sheet_properties.tabColor = SAPPHIRE

    _set_column_widths(
        ws,
        {
            1: 3,
            2: 22,
            3: 16,
            4: 16,
            5: 14,
            6: 14,
            7: 14,
            8: 18,
        },
    )

    industry = data.get("industry", "general_entry_level")
    industry_label = _get_industry_label(industry)
    locations = _get_locations(data)
    roles = _get_roles(data)
    client_name = data.get("client_name", "Client")
    competitors = data.get("competitors") or []
    if isinstance(competitors, str):
        competitors = [c.strip() for c in competitors.split(",") if c.strip()]

    synthesized = data.get("_synthesized", {})
    enriched = data.get("_enriched", {})

    row = 2

    # ── 1. Labour Market Overview ──
    row = _write_section_header(ws, row, "Labour Market Overview")

    labour_data = {}
    if research_mod:
        try:
            labour_data = research_mod.get_labour_market_intelligence(
                industry, locations
            )
        except Exception as exc:
            logger.warning("Labour market intelligence lookup failed: %s", exc)

    national = labour_data.get("national_summary", {})
    ind_metrics = labour_data.get("industry_metrics", {})

    # National Economic Snapshot -- use live data or hardcoded fallback
    row = _write_subsection_header(ws, row, "National Economic Snapshot")
    if national:
        display_fields = [
            ("Unemployment Rate", national.get("unemployment_rate") or ""),
            ("Job Openings", national.get("job_openings") or ""),
            ("Hires Rate", national.get("hires_rate") or ""),
            ("Quits Rate", national.get("quits_rate") or ""),
            (
                "Labor Force Participation",
                national.get("labor_force_participation") or "",
            ),
        ]
    else:
        # Fallback: latest available BLS/FRED figures (updated quarterly)
        display_fields = [
            ("Unemployment Rate", "4.0% (BLS, Q1 2026 est.)"),
            ("Job Openings", "~8.0M (JOLTS, latest available)"),
            ("Hires Rate", "3.4% (JOLTS, latest available)"),
            ("Quits Rate", "2.2% (JOLTS, latest available)"),
            ("Labor Force Participation", "62.5% (BLS, latest available)"),
            (
                "Note",
                "Live FRED/BLS data unavailable; figures are latest published estimates",
            ),
        ]
    for key, val in display_fields:
        val_str = _flatten_value(val)
        if val_str:
            row = _write_kv_row(ws, row, key, val_str)
    row += 1

    if ind_metrics:
        row = _write_subsection_header(ws, row, f"Industry Metrics: {industry_label}")
        for key, val in ind_metrics.items():
            if key in ("metadata", "source", "sources"):
                continue
            val_str = _flatten_value(val)
            if val_str:
                display_key = key.replace("_", " ").title()
                row = _write_kv_row(ws, row, display_key, val_str)
        row += 1

    # Location contexts from labour market data
    loc_contexts = labour_data.get("location_contexts") or []
    if loc_contexts:
        row = _write_subsection_header(ws, row, "Location Economic Context")
        headers = ["Location", "Country", "Unemployment", "Median Salary", "Context"]
        row = _write_table_header(ws, row, headers)
        for idx, lc in enumerate(loc_contexts):
            if isinstance(lc, dict):
                values = [
                    lc.get("location") or "",
                    lc.get("country") or "",
                    _flatten_value(lc.get("unemployment_rate") or ""),
                    _flatten_value(lc.get("median_salary") or ""),
                    lc.get("context_note") or ""[:80],
                ]
                row = _write_table_row(ws, row, values, alternate=idx % 2 == 1)
        row += 1

    row += 1

    # ── 2. Location Intelligence ──
    row = _write_section_header(ws, row, "Location Intelligence")

    loc_profiles = synthesized.get("location_profiles", {})
    loc_demographics = enriched.get("location_demographics", {})

    headers = [
        "Location",
        "Country",
        "Population",
        "Unemployment",
        "Median Income",
        "Key Industries",
    ]
    row = _write_table_header(ws, row, headers)

    for idx, loc in enumerate(locations):
        # Data cascade: synthesized > enriched > research fallback
        loc_data = {}

        # Try synthesized location profiles
        if loc_profiles:
            for loc_key, profile in loc_profiles.items():
                if isinstance(profile, dict) and (
                    loc.lower() in loc_key.lower() or loc_key.lower() in loc.lower()
                ):
                    loc_data = profile
                    break

        # Try enriched demographics
        if not loc_data and loc_demographics:
            if isinstance(loc_demographics, dict):
                for demo_key, demo_data in loc_demographics.items():
                    if isinstance(demo_data, dict) and (
                        loc.lower() in demo_key.lower()
                        or demo_key.lower() in loc.lower()
                    ):
                        loc_data = demo_data
                        break
            elif isinstance(loc_demographics, list):
                for demo_item in loc_demographics:
                    if isinstance(demo_item, dict):
                        demo_loc = demo_item.get(
                            "location", demo_item.get("name") or ""
                        )
                        if loc.lower() in str(demo_loc).lower():
                            loc_data = demo_item
                            break

        # Try research module as fallback
        if not loc_data and research_mod:
            try:
                loc_data = research_mod.get_location_info(loc) or {}
            except Exception:
                loc_data = {}

        # Extract values with fallback chain
        country = loc_data.get("country") or ""
        if not country:
            # Try to infer from location string
            parts = loc.split(",")
            country = parts[-1].strip() if len(parts) > 1 else "United States"

        # Prefer metro/city population over state-level population
        population = (
            loc_data.get("metro_population")
            or loc_data.get("city_population")
            or loc_data.get("population")
            or loc_data.get("pop")
            or ""
        )
        # Guard against state-level populations leaking through:
        # if the number is > 20M and location is a city, it's likely state-level
        if isinstance(population, (int, float)) and population > 20_000_000:
            # Use known metro populations for major US cities
            _metro_pop_fallback: Dict[str, str] = {
                "los angeles": "13.2M metro",
                "new york": "20.1M metro",
                "chicago": "9.5M metro",
                "dallas": "7.6M metro",
                "houston": "7.1M metro",
                "phoenix": "4.9M metro",
                "philadelphia": "6.2M metro",
                "san antonio": "2.6M metro",
                "san diego": "3.3M metro",
                "san jose": "2.0M metro",
                "san francisco": "4.7M metro",
                "seattle": "4.0M metro",
                "denver": "2.9M metro",
                "boston": "4.9M metro",
                "atlanta": "6.1M metro",
                "miami": "6.2M metro",
                "detroit": "4.3M metro",
                "minneapolis": "3.6M metro",
                "portland": "2.5M metro",
            }
            loc_lower = loc.lower()
            for city_key, metro_val in _metro_pop_fallback.items():
                if city_key in loc_lower:
                    population = metro_val
                    break
        unemployment = loc_data.get(
            "unemployment", loc_data.get("unemployment_rate") or ""
        )
        median_income = loc_data.get(
            "median_income",
            loc_data.get(
                "median_salary", loc_data.get("median_household_income") or ""
            ),
        )
        key_industries = loc_data.get(
            "key_industries",
            loc_data.get("major_employers", loc_data.get("top_industries") or ""),
        )

        pop_str = (
            _fmt_number(population)
            if isinstance(population, (int, float))
            else _flatten_value(population)
        )
        unemp_str = _flatten_value(unemployment)
        income_str = (
            _fmt_currency(median_income)
            if isinstance(median_income, (int, float))
            else _flatten_value(median_income)
        )
        industry_str = _flatten_value(key_industries)

        values = [
            loc,
            country,
            pop_str or "N/A",
            unemp_str or "N/A",
            income_str or "N/A",
            industry_str[:80] if industry_str else "N/A",
        ]

        row = _write_table_row(ws, row, values, alternate=idx % 2 == 1)

    row += 2

    # ── 3. Competitive Landscape ──
    row = _write_section_header(ws, row, "Competitive Landscape")

    comp_intel = synthesized.get("competitive_intelligence", {})
    company_profile = comp_intel.get("company_profile", {})
    sec_data = enriched.get("sec_data", {})

    # Company profile section
    if company_profile or sec_data:
        row = _write_subsection_header(ws, row, f"Company Profile: {client_name}")
        profile_fields = {}

        # Merge from sec_data and company_profile
        if isinstance(sec_data, dict):
            profile_fields.update(
                {
                    "Company Name": sec_data.get(
                        "name", sec_data.get("company_name", client_name)
                    ),
                    "CIK": sec_data.get("cik") or "",
                    "SIC Code": sec_data.get("sic", sec_data.get("sic_code") or ""),
                    "SIC Description": sec_data.get("sic_description") or "",
                    "State": sec_data.get(
                        "state", sec_data.get("state_of_incorporation") or ""
                    ),
                    "Fiscal Year End": sec_data.get("fiscal_year_end") or "",
                }
            )

        if isinstance(company_profile, dict):
            for k, v in company_profile.items():
                if k not in ("metadata", "source") and v:
                    profile_fields[k.replace("_", " ").title()] = v

        for key, val in profile_fields.items():
            val_str = _flatten_value(val)
            if val_str:
                row = _write_kv_row(ws, row, key, val_str)
        row += 1

    # Competitors table
    comp_analysis = comp_intel.get(
        "competitors", comp_intel.get("competitor_analysis") or []
    )
    if not comp_analysis and competitors:
        # Build minimal competitor entries from names list
        comp_analysis = [{"name": c} for c in competitors]

    # Fallback: use industry top employers from knowledge base
    if not comp_analysis:
        _industry_top_employers: Dict[str, List[str]] = {
            "healthcare_medical": [
                "HCA Healthcare",
                "UnitedHealth Group",
                "Ascension",
                "CommonSpirit Health",
                "Kaiser Permanente",
            ],
            "tech_engineering": ["Google", "Amazon", "Microsoft", "Meta", "Apple"],
            "finance_banking": [
                "JPMorgan Chase",
                "Bank of America",
                "Goldman Sachs",
                "Citigroup",
                "Wells Fargo",
            ],
            "retail_consumer": ["Walmart", "Amazon", "Costco", "Target", "Home Depot"],
            "aerospace_defense": [
                "Lockheed Martin",
                "Boeing",
                "Raytheon",
                "Northrop Grumman",
                "General Dynamics",
            ],
            "logistics_supply_chain": [
                "UPS",
                "FedEx",
                "Amazon Logistics",
                "XPO Logistics",
                "C.H. Robinson",
            ],
            "pharma_biotech": [
                "Pfizer",
                "Johnson & Johnson",
                "AbbVie",
                "Merck",
                "Amgen",
            ],
            "hospitality_travel": ["Marriott", "Hilton", "Hyatt", "IHG", "Airbnb"],
            "education": [
                "Pearson",
                "McGraw-Hill",
                "Chegg",
                "Coursera",
                "University Systems",
            ],
            "energy_utilities": [
                "ExxonMobil",
                "Chevron",
                "NextEra Energy",
                "Duke Energy",
                "Southern Company",
            ],
        }
        fallback_names = _industry_top_employers.get(industry, [])
        if fallback_names:
            comp_analysis = [
                {
                    "name": n,
                    "industry": industry_label,
                    "size": "",
                    "hiring_activity": "Active (est.)",
                    "overlap_score": "",
                }
                for n in fallback_names
            ]

    if comp_analysis:
        row = _write_subsection_header(ws, row, "Competitor Analysis")
        headers = ["Name", "Industry", "Size", "Hiring Activity", "Overlap Score"]
        row = _write_table_header(ws, row, headers)

        comp_list = comp_analysis if isinstance(comp_analysis, list) else []
        if isinstance(comp_analysis, dict):
            comp_list = [
                {"name": k, **v} if isinstance(v, dict) else {"name": k}
                for k, v in comp_analysis.items()
            ]

        for idx, comp in enumerate(comp_list[:10]):
            if isinstance(comp, dict):
                values = [
                    comp.get("name", comp.get("company") or ""),
                    _flatten_value(comp.get("industry") or ""),
                    _flatten_value(comp.get("size", comp.get("employee_count") or "")),
                    _flatten_value(
                        comp.get("hiring_activity", comp.get("hiring_channels") or "")
                    ),
                    _flatten_value(
                        comp.get("overlap_score", comp.get("overlap") or "")
                    ),
                ]
            elif isinstance(comp, str):
                values = [comp, "", "", "", ""]
            else:
                continue
            row = _write_table_row(ws, row, values, alternate=idx % 2 == 1)

        row += 1

    # Market positioning summary
    market_pos = comp_intel.get("market_positioning", comp_intel.get("summary") or "")
    if market_pos:
        row = _write_subsection_header(ws, row, "Market Positioning")
        ws.merge_cells(
            start_row=row, start_column=COL_START, end_row=row, end_column=COL_END
        )
        cell = ws.cell(row=row, column=COL_START, value=_flatten_value(market_pos))
        cell.font = _FONT_BODY
        cell.alignment = _ALIGN_WRAP
        row += 1

    row += 2

    # ── 4. Salary Intelligence ──
    salary_intel = synthesized.get("salary_intelligence", {})

    if salary_intel:
        row = _write_section_header(ws, row, "Salary Intelligence")

        headers = ["Role", "Min", "P25", "Median", "P75", "Max", "Confidence"]
        row = _write_table_header(ws, row, headers)

        salary_items = salary_intel
        if isinstance(salary_intel, dict):
            # Could be keyed by role name or be a list
            if isinstance(next(iter(salary_intel.values()), None), dict):
                salary_items = salary_intel
            else:
                salary_items = {"All Roles": salary_intel}

        for idx, (role_key, sal_data) in enumerate(
            salary_items.items()
            if isinstance(salary_items, dict)
            else enumerate(salary_items)
        ):
            if isinstance(sal_data, dict):
                role_name = sal_data.get("role", sal_data.get("title", role_key))
                confidence = _safe_num(
                    sal_data.get("confidence", sal_data.get("confidence_score", 0.5))
                )
                is_low_conf = confidence < 0.5

                values = [
                    role_name if isinstance(role_name, str) else str(role_name),
                    _fmt_currency(sal_data.get("min", sal_data.get("p10") or 0)),
                    _fmt_currency(sal_data.get("p25") or 0),
                    _fmt_currency(sal_data.get("median", sal_data.get("p50") or 0)),
                    _fmt_currency(sal_data.get("p75") or 0),
                    _fmt_currency(sal_data.get("max", sal_data.get("p90") or 0)),
                    f"{confidence:.0%}",
                ]

                # Highlight low-confidence rows
                row_fill = _FILL_AMBER_BG if is_low_conf else None
                fills_list = [row_fill] * len(values) if is_low_conf else None
                conf_font = [None] * 6 + [
                    _grade_font("C" if confidence >= 0.5 else "D")
                ]

                row = _write_table_row(
                    ws,
                    row,
                    values,
                    alternate=idx % 2 == 1,
                    fills=fills_list,
                    fonts=conf_font,
                )

        # Sources footnote
        sources = set()
        for sal_data in (
            salary_intel.values() if isinstance(salary_intel, dict) else []
        ):
            if isinstance(sal_data, dict):
                src = sal_data.get("sources", sal_data.get("source") or "")
                if src:
                    if isinstance(src, list):
                        sources.update(src)
                    else:
                        sources.add(str(src))
        if sources:
            row = _write_footnote(ws, row, f"Sources: {', '.join(sorted(sources))}")

    row += 2

    # ── 5. Market Demand ──
    market_demand = synthesized.get("job_market_demand", {})

    if market_demand:
        row = _write_section_header(ws, row, "Market Demand by Role")

        headers = [
            "Role",
            "Postings",
            "Talent Pool",
            "Competition",
            "Temperature",
            "Trend",
        ]
        row = _write_table_header(ws, row, headers)

        demand_items = market_demand
        if isinstance(market_demand, dict) and not all(
            isinstance(v, dict) for v in market_demand.values()
        ):
            demand_items = {"All": market_demand}

        for idx, (role_key, demand) in enumerate(
            demand_items.items()
            if isinstance(demand_items, dict)
            else enumerate(demand_items)
        ):
            if isinstance(demand, dict):
                role_name = demand.get("role", demand.get("title", role_key))
                values = [
                    role_name if isinstance(role_name, str) else str(role_name),
                    _fmt_number(
                        demand.get("postings", demand.get("job_postings") or 0)
                    ),
                    _fmt_number(demand.get("talent_pool", demand.get("supply") or 0)),
                    _flatten_value(
                        demand.get("competition", demand.get("competition_level") or "")
                    ),
                    _flatten_value(
                        demand.get(
                            "temperature", demand.get("market_temperature") or ""
                        )
                    ),
                    _flatten_value(
                        demand.get("trend", demand.get("trend_direction") or "")
                    ),
                ]
                row = _write_table_row(ws, row, values, alternate=idx % 2 == 1)

    row += 2

    # ── 6. Workforce Trends ──
    workforce = synthesized.get("workforce_insights", {})

    if workforce:
        row = _write_section_header(ws, row, "Workforce Trends")

        # CRITICAL: Properly flatten nested structures -- never use str() on dicts
        for section_key, section_val in workforce.items():
            if section_key in ("metadata", "source", "sources", "confidence"):
                continue

            section_label = section_key.replace("_", " ").title()

            if isinstance(section_val, dict):
                row = _write_subsection_header(ws, row, section_label)
                for k, v in section_val.items():
                    if k in ("metadata", "source"):
                        continue
                    val_str = _flatten_value(v)
                    if val_str:
                        row = _write_kv_row(
                            ws, row, k.replace("_", " ").title(), val_str
                        )
                row += 1

            elif isinstance(section_val, list):
                row = _write_subsection_header(ws, row, section_label)
                for item in section_val[:8]:
                    val_str = _flatten_value(item)
                    if val_str:
                        ws.merge_cells(
                            start_row=row,
                            start_column=COL_START,
                            end_row=row,
                            end_column=COL_END,
                        )
                        cell = ws.cell(
                            row=row, column=COL_START, value=f"  - {val_str}"
                        )
                        cell.font = _FONT_BODY
                        cell.alignment = _ALIGN_WRAP
                        row += 1
                row += 1

            elif isinstance(section_val, (str, int, float, bool)):
                row = _write_kv_row(ws, row, section_label, _flatten_value(section_val))

    row += 2
    _write_attribution_footer(ws, row)


# ═══════════════════════════════════════════════════════════════════════════════
# SHEET 4: SOURCES & DATA CONFIDENCE
# ═══════════════════════════════════════════════════════════════════════════════


def _build_sheet_sources(ws, data: dict):
    """Build Sheet 4: Sources & Data Confidence."""
    ws.title = "Sources & Confidence"
    ws.sheet_properties.tabColor = SAPPHIRE

    _set_column_widths(
        ws,
        {
            1: 3,
            2: 22,
            3: 14,
            4: 14,
            5: 14,
            6: 14,
            7: 14,
            8: 18,
        },
    )

    synthesized = data.get("_synthesized", {})
    enriched = data.get("_enriched", {})
    confidence_scores = synthesized.get("confidence_scores", {})
    data_quality = synthesized.get("data_quality", {})
    enrichment_summary = enriched.get("enrichment_summary", {})

    row = 2

    # ── 1. Overall Confidence Grade ──
    row = _write_section_header(ws, row, "Data Confidence Assessment")

    overall_score = _safe_num(
        confidence_scores.get(
            "overall", confidence_scores.get("overall_confidence", 0.5)
        )
    )
    overall_grade = _grade_from_score(overall_score)

    # Store computed confidence so PPT uses the same value
    data["_computed_confidence_pct"] = round(overall_score * 100)

    # Large grade display
    ws.merge_cells(
        start_row=row, start_column=COL_START, end_row=row + 2, end_column=COL_START + 1
    )
    grade_cell = ws.cell(row=row, column=COL_START, value=overall_grade)
    grade_cell.font = _FONT_GRADE_LARGE
    if overall_grade in ("A", "B"):
        grade_cell.fill = PatternFill(
            start_color=GREEN, end_color=GREEN, fill_type="solid"
        )
    elif overall_grade == "C":
        grade_cell.fill = PatternFill(
            start_color=AMBER, end_color=AMBER, fill_type="solid"
        )
    else:
        grade_cell.fill = PatternFill(start_color=RED, end_color=RED, fill_type="solid")
    grade_cell.alignment = _ALIGN_CENTER

    # Grade description next to it
    ws.merge_cells(
        start_row=row, start_column=COL_START + 2, end_row=row, end_column=COL_END
    )
    desc_cell = ws.cell(
        row=row, column=COL_START + 2, value=f"Overall Confidence: {overall_score:.0%}"
    )
    desc_cell.font = _FONT_HERO
    desc_cell.alignment = _ALIGN_LEFT

    ws.merge_cells(
        start_row=row + 1,
        start_column=COL_START + 2,
        end_row=row + 1,
        end_column=COL_END,
    )
    quality_msg = data_quality.get("message", data_quality.get("summary") or "")
    if not quality_msg:
        if overall_grade in ("A", "B"):
            quality_msg = "High-quality data from multiple verified sources"
        elif overall_grade == "C":
            quality_msg = "Moderate data quality -- some sections rely on benchmarks"
        else:
            quality_msg = "Limited data availability -- results should be validated"

    qual_cell = ws.cell(row=row + 1, column=COL_START + 2, value=quality_msg)
    qual_cell.font = _FONT_BODY
    qual_cell.alignment = _ALIGN_WRAP

    row += 4

    # ── 2. Per-Section Confidence ──
    section_scores = confidence_scores.get(
        "sections", confidence_scores.get("per_section", {})
    )

    if section_scores and isinstance(section_scores, dict):
        row = _write_section_header(ws, row, "Per-Section Confidence")

        headers = ["Section", "Score", "Grade", "Sources"]
        row = _write_table_header(ws, row, headers)

        for idx, (section, score_data) in enumerate(section_scores.items()):
            if isinstance(score_data, dict):
                score = _safe_num(
                    score_data.get("score", score_data.get("confidence") or 0)
                )
                sources = score_data.get(
                    "sources", score_data.get("data_sources") or []
                )
                sources_str = _flatten_value(sources) if sources else ""
            elif isinstance(score_data, (int, float)):
                score = float(score_data)
                sources_str = ""
            else:
                continue

            grade = _grade_from_score(score)
            values = [
                section.replace("_", " ").title(),
                f"{score:.0%}",
                grade,
                sources_str[:60],
            ]

            grade_f = _grade_fill(grade)
            g_font = _grade_font(grade)
            fills_list = [None, None, grade_f, None]
            fonts_list = [None, None, g_font, None]
            row = _write_table_row(
                ws,
                row,
                values,
                alternate=idx % 2 == 1,
                fills=fills_list,
                fonts=fonts_list,
            )

    row += 2

    # ── 3. Source Assessment ──
    row = _write_section_header(ws, row, "Source Assessment & Bias Analysis")

    # Collect all sources mentioned in the data
    all_sources = set()

    # Internal metadata keys that should never appear as data sources
    _METADATA_BLOCKLIST = {
        "api_details",
        "apis_called",
        "apis_circuit_broken",
        "apis_failed",
        "apis_skipped",
        "apis_succeeded",
        "cached",
        "confidence_score",
        "total_time_seconds",
        "request_id",
        "plan_id",
        "metadata",
        "source",
    }

    # From enrichment summary
    if isinstance(enrichment_summary, dict):
        for api_name, api_status in enrichment_summary.items():
            if api_name in _METADATA_BLOCKLIST:
                continue
            if isinstance(api_status, dict):
                source_name = api_status.get("source_name", api_name)
                if source_name not in _METADATA_BLOCKLIST:
                    all_sources.add(source_name)
            else:
                if str(api_name) not in _METADATA_BLOCKLIST:
                    all_sources.add(str(api_name))

    # From known API sources used in the pipeline
    known_sources = [
        "BLS OES",
        "US Census ACS",
        "O*NET",
        "SEC EDGAR",
        "Google Ads API",
        "Meta Marketing API",
        "LinkedIn Marketing API",
        "Bing Ads API",
        "TikTok Marketing API",
        "Google Trends",
        "DataUSA",
        "GeoNames",
        "World Bank",
        "IMF",
        "Clearbit",
        "Wikipedia",
        "CareerOneStop API",
        "Joveo Campaign Data",
        "Appcast Benchmark 2026",
    ]
    all_sources.update(known_sources)

    # Remove empty strings
    all_sources.discard("")

    if all_sources:
        headers = ["Source", "Category", "Confidence", "Bias Assessment"]
        row = _write_table_header(ws, row, headers)

        sorted_sources = sorted(all_sources)
        for idx, source in enumerate(sorted_sources):
            assessment = assess_source_bias(source)
            values = [
                source,
                assessment["category"],
                f"{assessment['confidence_modifier']:.0%}",
                assessment["bias"],
            ]

            # Color-code by confidence modifier
            conf_mod = assessment["confidence_modifier"]
            if conf_mod >= 0.9:
                conf_fill = _FILL_GREEN_BG
            elif conf_mod >= 0.75:
                conf_fill = _FILL_BLUE_PALE
            elif conf_mod >= 0.65:
                conf_fill = _FILL_AMBER_BG
            else:
                conf_fill = _FILL_RED_BG

            fills_list = [None, None, conf_fill, None]
            row = _write_table_row(
                ws, row, values, alternate=idx % 2 == 1, fills=fills_list
            )

    row += 2

    # ── 4. API Status Report ──
    row = _write_section_header(ws, row, "API Status Report")

    if enrichment_summary and isinstance(enrichment_summary, dict):
        headers = ["API", "Status", "Records", "Notes"]
        row = _write_table_header(ws, row, headers)

        for idx, (api_key, api_status) in enumerate(enrichment_summary.items()):
            if isinstance(api_status, dict):
                status = api_status.get("status", "unknown")
                records = api_status.get("records", api_status.get("count") or "")
                notes = api_status.get("error", api_status.get("message") or "")
            elif isinstance(api_status, bool):
                status = "success" if api_status else "failed"
                records = ""
                notes = ""
            elif isinstance(api_status, str):
                status = api_status
                records = ""
                notes = ""
            else:
                status = str(api_status)
                records = ""
                notes = ""

            # Normalize status display
            status_display = status.title() if isinstance(status, str) else str(status)
            is_success = str(status).lower() in ("success", "ok", "true", "completed")

            values = [
                api_key.replace("_", " ").title(),
                status_display,
                _flatten_value(records),
                _flatten_value(notes)[:60],
            ]

            status_fill = _FILL_GREEN_BG if is_success else _FILL_RED_BG
            fills_list = [None, status_fill, None, None]
            status_font = Font(
                name="Calibri", bold=True, size=10, color=GREEN if is_success else RED
            )
            fonts_list = [None, status_font, None, None]
            row = _write_table_row(
                ws,
                row,
                values,
                alternate=idx % 2 == 1,
                fills=fills_list,
                fonts=fonts_list,
            )
    else:
        ws.merge_cells(
            start_row=row, start_column=COL_START, end_row=row, end_column=COL_END
        )
        ws.cell(
            row=row, column=COL_START, value="No API enrichment summary available."
        ).font = _FONT_BODY
        row += 1

    row += 2

    # ── 5. Methodology Notes ──
    row = _write_section_header(ws, row, "Methodology & Data Hierarchy")

    methodology_items = [
        (
            "Priority 1: Client Data",
            "Client-provided data (uploaded briefs, historical campaign data) takes highest precedence.",
        ),
        (
            "Priority 2: Live APIs",
            "Real-time data from 25+ APIs (BLS, Census, SEC, Google Ads, Meta, LinkedIn, etc.) "
            "provides current market signals.",
        ),
        (
            "Priority 3: KB Benchmarks",
            "Knowledge base benchmarks (Appcast 2026, Google Ads 2025, industry reports) provide "
            "validated baseline data.",
        ),
        (
            "Priority 4: Embedded Fallbacks",
            "Curated fallback data embedded in the research module ensures coverage when APIs "
            "are unavailable.",
        ),
    ]

    for key, desc in methodology_items:
        row = _write_kv_row(ws, row, key, desc)

    row += 1

    # Data quality note
    ws.merge_cells(
        start_row=row, start_column=COL_START, end_row=row, end_column=COL_END
    )
    note_cell = ws.cell(
        row=row,
        column=COL_START,
        value="Note: Job boards and recruitment marketing vendors are flagged as "
        "'Potential Conflict of Interest' with lower confidence weighting. "
        "Academic and government sources (BLS, Census, SHRM) receive "
        "'Independent / High Trust' classification.",
    )
    note_cell.font = _FONT_FOOTNOTE
    note_cell.alignment = _ALIGN_WRAP
    row += 2

    # Geopolitical context (if available)
    geo_context = synthesized.get("geopolitical_context", {})
    if geo_context and isinstance(geo_context, dict):
        row = _write_subsection_header(ws, row, "Geopolitical Context")
        for key, val in geo_context.items():
            if key in ("metadata", "source"):
                continue
            val_str = _flatten_value(val)
            if val_str:
                row = _write_kv_row(ws, row, key.replace("_", " ").title(), val_str)


# ═══════════════════════════════════════════════════════════════════════════════
# SHEET 5: ROI PROJECTIONS
# ═══════════════════════════════════════════════════════════════════════════════

# Channel-type conversion rates (application-to-hire %)
_ROI_CONVERSION_RATES: Dict[str, Tuple[float, float]] = {
    "job_board": (0.08, 0.12),
    "programmatic": (0.05, 0.08),
    "social": (0.03, 0.06),
    "niche_board": (0.10, 0.15),
    "regional": (0.06, 0.10),
    "search": (0.05, 0.08),
    "display": (0.03, 0.06),
    "employer_branding": (0.04, 0.08),
    "career_site": (0.08, 0.12),
    "referral": (0.15, 0.25),
    "events": (0.10, 0.18),
    "staffing": (0.12, 0.20),
    "email": (0.05, 0.10),
}

# Time-to-fill estimates by channel type (days)
_ROI_TIME_TO_FILL: Dict[str, Tuple[int, int]] = {
    "programmatic": (25, 35),
    "job_board": (30, 45),
    "social": (35, 50),
    "niche_board": (20, 30),
    "regional": (30, 45),
    "search": (30, 40),
    "display": (35, 50),
    "employer_branding": (40, 60),
    "career_site": (25, 40),
    "referral": (15, 25),
    "events": (30, 50),
    "staffing": (20, 35),
    "email": (30, 45),
}


def _roi_category_for_channel(channel_name: str) -> str:
    """Map a channel name to its ROI category key for conversion/time estimates."""
    name_lower = channel_name.lower()
    mapping: Dict[str, str] = {
        "programmatic": "programmatic",
        "dsp": "programmatic",
        "global_boards": "job_board",
        "global job": "job_board",
        "job board": "job_board",
        "indeed": "job_board",
        "niche": "niche_board",
        "specialty": "niche_board",
        "social": "social",
        "linkedin": "social",
        "meta": "social",
        "facebook": "social",
        "regional": "regional",
        "local": "regional",
        "employer_branding": "employer_branding",
        "employer brand": "employer_branding",
        "career_site": "career_site",
        "career site": "career_site",
        "referral": "referral",
        "event": "events",
        "staffing": "staffing",
        "agency": "staffing",
        "search": "search",
        "sem": "search",
        "display": "display",
        "banner": "display",
        "email": "email",
        "apac": "regional",
        "emea": "regional",
    }
    for keyword, category in mapping.items():
        if keyword in name_lower:
            return category
    return "job_board"


# ---------------------------------------------------------------------------
# Role difficulty -> base time-to-fill adjustments (days)
# ---------------------------------------------------------------------------
_ROLE_DIFFICULTY_TTF: dict[str, tuple[int, int]] = {
    "executive": (60, 90),
    "c-suite": (60, 90),
    "vp": (60, 90),
    "director": (45, 75),
    "tech": (45, 60),
    "engineering": (45, 60),
    "software": (45, 60),
    "data_science": (45, 60),
    "cybersecurity": (45, 60),
    "nursing": (30, 45),
    "healthcare": (30, 45),
    "medical": (30, 45),
    "rn": (30, 45),
    "lpn": (30, 45),
    "cna": (25, 35),
    "hourly": (14, 21),
    "entry": (14, 21),
    "retail": (14, 21),
    "warehouse": (14, 21),
    "food_service": (14, 21),
    "cdl": (21, 30),
    "trucking": (21, 30),
    "driver": (21, 30),
    "logistics": (21, 30),
}


def _compute_dynamic_ttf(channel_base_ttf: int, data: dict) -> int:
    """Compute dynamic time-to-fill by adjusting channel base with role/volume/market factors.

    Factors applied:
    - Role difficulty: executive (60-90d), tech (45-60d), nursing (30-45d),
      hourly (14-21d), CDL/trucking (21-30d)
    - Volume: >50 hires adds 15-30 days proportionally
    - Market conditions: 'drought' adds 10 days, 'surplus' subtracts 5

    Args:
        channel_base_ttf: Midpoint time-to-fill for the channel type (days).
        data: Full plan data dict with industry, roles, hire_volume, etc.

    Returns:
        Adjusted time-to-fill in days (minimum 10).
    """
    industry = str(data.get("industry") or "").lower()
    roles_raw = data.get("target_roles") or data.get("roles") or []

    # ── Role difficulty adjustment ──
    role_adjustment: float = 1.0
    role_texts: list[str] = []
    for r in (roles_raw if isinstance(roles_raw, list) else []):
        if isinstance(r, str):
            role_texts.append(r.lower())
        elif isinstance(r, dict):
            role_texts.append(str(r.get("title") or "").lower())

    combined_role_text = " ".join(role_texts) + f" {industry}"

    # Find best matching role difficulty
    matched_range: tuple[int, int] | None = None
    for keyword, ttf_range in _ROLE_DIFFICULTY_TTF.items():
        if keyword in combined_role_text:
            matched_range = ttf_range
            break

    if matched_range:
        role_midpoint = (matched_range[0] + matched_range[1]) / 2.0
        # Scale channel TTF toward the role-appropriate range
        # Blend: 60% role-driven, 40% channel-driven
        adjusted_ttf = int(role_midpoint * 0.6 + channel_base_ttf * 0.4)
    else:
        adjusted_ttf = channel_base_ttf

    # ── Volume adjustment: >50 hires extends timeline ──
    try:
        hire_vol_str = str(data.get("hire_volume") or "0")
        hire_vol = int(hire_vol_str.replace(",", "").replace("+", "").strip() or "0")
    except (ValueError, TypeError):
        hire_vol = 0

    if hire_vol > 200:
        adjusted_ttf += 30
    elif hire_vol > 100:
        adjusted_ttf += 22
    elif hire_vol > 50:
        adjusted_ttf += 15

    # ── Market condition adjustment ──
    synthesized = data.get("_synthesized", {})
    market_condition = str(synthesized.get("market_condition") or "").lower()
    if "drought" in market_condition or "tight" in market_condition:
        adjusted_ttf += 10
    elif "surplus" in market_condition or "favorable" in market_condition:
        adjusted_ttf -= 5

    return max(10, adjusted_ttf)


def _build_sheet_roi_projections(ws, data: dict) -> None:
    """Build Sheet 5: ROI Projections with per-channel hire projections and efficiency scores.

    Reads channel allocation data from _budget_allocation and computes:
    - Projected applications and hires per channel
    - Cost per hire and time-to-fill estimates
    - ROI efficiency scores (1-10)
    - Summary totals row
    """
    ws.title = "ROI Projections"
    ws.sheet_properties.tabColor = GREEN

    _set_column_widths(
        ws,
        {
            1: 3,  # margin
            2: 24,  # Channel Name
            3: 16,  # Budget Allocated
            4: 18,  # Projected Applications
            5: 16,  # Projected Hires
            6: 16,  # Cost Per Hire
            7: 18,  # Est. Time to Fill
            8: 12,  # ROI Score
        },
    )

    budget_alloc = data.get("_budget_allocation", {})
    channel_allocs = budget_alloc.get("channel_allocations", {})

    row = 2

    # ── Section Header ──
    row = _write_section_header(ws, row, "ROI Projections & Hire Forecast")

    # ── Summary Cards (computed after channel loop, written first) ──
    summary_row_start = row
    row += 2  # reserve 2 rows for summary

    # ── Gather ROI data per channel ──
    roi_rows: List[Dict[str, Any]] = []
    total_budget = 0.0
    total_projected_hires = 0
    total_projected_apps = 0
    sum_cph = 0.0
    sum_ttf = 0.0
    channels_with_hires = 0

    sorted_channels = sorted(
        channel_allocs.items(),
        key=lambda x: x[1].get("dollar_amount", x[1].get("dollars") or 0),
        reverse=True,
    )

    for ch_name, ch_data in sorted_channels:
        try:
            dollars = ch_data.get("dollar_amount", ch_data.get("dollars") or 0)
            if not dollars or dollars <= 0:
                continue

            category = _roi_category_for_channel(ch_name)

            # S24: CPA estimate with role + location difficulty multipliers.
            # S23 CPA floors ($35-75) produced unrealistically low cost/hire
            # for professional roles ($732/hire for SWE in NYC, real is $5K-15K).
            existing_cpa = ch_data.get("cpa") or 0
            if existing_cpa and existing_cpa > 0:
                cpa_estimate = existing_cpa
            else:
                # Base CPA floors by channel category
                _CPA_FLOORS = {
                    "programmatic": 45.0,
                    "job_board": 35.0,
                    "social": 65.0,
                    "niche_board": 50.0,
                    "search": 55.0,
                    "display": 40.0,
                    "employer_branding": 75.0,
                    "career_site": 30.0,
                    "referral": 20.0,
                    "regional": 40.0,
                }
                _cpa_floor = _CPA_FLOORS.get(category, 40.0)

                # Role difficulty multiplier -- professional roles have much higher CPAs
                _role_lower = str(
                    data.get("role") or data.get("job_title") or ""
                ).lower()
                _ROLE_CPA_MULTIPLIER = 1.0
                if any(
                    k in _role_lower
                    for k in (
                        "engineer",
                        "developer",
                        "architect",
                        "devops",
                        "sre",
                        "data scientist",
                        "machine learning",
                    )
                ):
                    _ROLE_CPA_MULTIPLIER = 3.0
                elif any(
                    k in _role_lower
                    for k in (
                        "director",
                        "vp",
                        "vice president",
                        "head of",
                        "chief",
                        "executive",
                        "cto",
                        "cfo",
                        "cio",
                    )
                ):
                    _ROLE_CPA_MULTIPLIER = 4.0
                elif any(
                    k in _role_lower
                    for k in ("manager", "lead", "senior", "principal", "staff")
                ):
                    _ROLE_CPA_MULTIPLIER = 2.0
                elif any(
                    k in _role_lower
                    for k in (
                        "nurse",
                        "physician",
                        "pharmacist",
                        "therapist",
                        "surgeon",
                    )
                ):
                    _ROLE_CPA_MULTIPLIER = 2.5

                # Location cost multiplier -- high-cost metros
                _loc_lower = str(data.get("location") or "").lower()
                _LOC_CPA_MULTIPLIER = 1.0
                if any(
                    c in _loc_lower
                    for c in (
                        "new york",
                        "nyc",
                        "san francisco",
                        "sf",
                        "silicon valley",
                        "seattle",
                        "boston",
                        "washington dc",
                        "los angeles",
                    )
                ):
                    _LOC_CPA_MULTIPLIER = 1.5
                elif any(
                    c in _loc_lower
                    for c in (
                        "chicago",
                        "denver",
                        "dallas",
                        "atlanta",
                        "austin",
                        "miami",
                        "portland",
                    )
                ):
                    _LOC_CPA_MULTIPLIER = 1.2

                cpa_estimate = max(
                    _cpa_floor * _ROLE_CPA_MULTIPLIER * _LOC_CPA_MULTIPLIER, 40.0
                )

            projected_apps = (
                max(1, int(dollars / cpa_estimate)) if cpa_estimate > 0 else 0
            )

            # Use existing projected apps if available and reasonable
            existing_apps = ch_data.get("projected_applications") or 0
            if existing_apps > 0:
                projected_apps = existing_apps

            # Conversion rate: midpoint of channel-type range
            conv_lo, conv_hi = _ROI_CONVERSION_RATES.get(category, (0.05, 0.10))
            conversion_rate = (conv_lo + conv_hi) / 2.0

            projected_hires = max(0, int(projected_apps * conversion_rate))

            # CRITICAL: Use upstream projected_hires when available to stay
            # consistent with the Executive Summary sheet (which reads the
            # same ch_data.get("projected_hires") value directly).
            existing_hires = ch_data.get("projected_hires") or 0
            if existing_hires > 0:
                projected_hires = existing_hires

            cost_per_hire = round(dollars / max(projected_hires, 1), 2)

            # Time to fill: channel midpoint adjusted for role/volume/market
            ttf_lo, ttf_hi = _ROI_TIME_TO_FILL.get(category, (30, 45))
            base_ttf = (ttf_lo + ttf_hi) // 2
            est_time_to_fill = _compute_dynamic_ttf(base_ttf, data)

            # ROI Score (1-10): inversely proportional to cost-per-hire
            # Uses realistic recruitment industry thresholds:
            #   <$300 CPH = 10, $300-600 = 9, $600-1000 = 8, $1000-1500 = 7,
            #   $1500-2500 = 6, $2500-4000 = 5, $4000-6000 = 4, $6000-10000 = 3,
            #   $10000-20000 = 2, >$20000 = 1
            existing_roi = ch_data.get("roi_score") or 0
            if existing_roi and 1 <= existing_roi <= 10:
                roi_score = existing_roi
            else:
                if cost_per_hire <= 300:
                    roi_score = 10
                elif cost_per_hire <= 600:
                    roi_score = 9
                elif cost_per_hire <= 1000:
                    roi_score = 8
                elif cost_per_hire <= 1500:
                    roi_score = 7
                elif cost_per_hire <= 2500:
                    roi_score = 6
                elif cost_per_hire <= 4000:
                    roi_score = 5
                elif cost_per_hire <= 6000:
                    roi_score = 4
                elif cost_per_hire <= 10000:
                    roi_score = 3
                elif cost_per_hire <= 20000:
                    roi_score = 2
                else:
                    roi_score = 1

            roi_rows.append(
                {
                    "name": ch_name.replace("_", " ").title(),
                    "budget": dollars,
                    "projected_apps": projected_apps,
                    "projected_hires": projected_hires,
                    "cost_per_hire": cost_per_hire,
                    "time_to_fill": est_time_to_fill,
                    "roi_score": roi_score,
                    "category": category,
                    "conversion_rate": conversion_rate,
                }
            )

            total_budget += dollars
            total_projected_hires += projected_hires
            total_projected_apps += projected_apps
            if projected_hires > 0:
                sum_cph += cost_per_hire
                sum_ttf += est_time_to_fill
                channels_with_hires += 1

        except Exception as exc:
            logger.warning("ROI projection failed for channel %s: %s", ch_name, exc)
            continue

    # Cost/Hire = total_budget / total_hires (consistent with Executive Summary)
    avg_cph = round(total_budget / max(total_projected_hires, 1), 2)
    avg_ttf = round(sum_ttf / max(channels_with_hires, 1))

    # ── Write summary row at reserved position ──
    summary_labels = [
        "Total Budget",
        "Total Proj. Hires",
        "Avg Cost/Hire",
        "Avg Time to Fill",
    ]
    summary_values = [
        f"${total_budget:,.0f}",
        str(total_projected_hires),
        f"${avg_cph:,.0f}",
        f"{avg_ttf} days",
    ]

    for i, (label, value) in enumerate(zip(summary_labels, summary_values)):
        col = COL_START + i
        # Label row
        cell_l = ws.cell(row=summary_row_start, column=col, value=label)
        cell_l.font = _FONT_METRIC_LABEL
        cell_l.alignment = _ALIGN_CENTER
        cell_l.fill = _FILL_BLUE_PALE
        # Value row
        cell_v = ws.cell(row=summary_row_start + 1, column=col, value=value)
        cell_v.font = _FONT_METRIC_VALUE
        cell_v.alignment = _ALIGN_CENTER
        cell_v.fill = _FILL_WHITE
        cell_v.border = _BORDER_THIN

    row = summary_row_start + 3

    # ── Channel ROI Table ──
    row = _write_subsection_header(ws, row, "Per-Channel ROI Analysis")

    headers = [
        "Channel Name",
        "Budget ($)",
        "Proj. Applications",
        "Proj. Hires",
        "Cost Per Hire",
        "Time to Fill",
        "ROI Score",
    ]
    row = _write_table_header(ws, row, headers)

    for idx, roi_data in enumerate(roi_rows):
        roi_score = roi_data["roi_score"]
        # Color-code ROI score
        if roi_score >= 7:
            score_font = Font(name="Calibri", bold=True, size=10, color=GREEN)
            score_fill = _FILL_GREEN_BG
        elif roi_score >= 4:
            score_font = Font(name="Calibri", bold=True, size=10, color=AMBER)
            score_fill = _FILL_AMBER_BG
        else:
            score_font = Font(name="Calibri", bold=True, size=10, color=RED)
            score_fill = _FILL_RED_BG

        values = [
            roi_data["name"],
            f"${roi_data['budget']:,.0f}",
            f"{roi_data['projected_apps']:,}",
            str(roi_data["projected_hires"]),
            f"${roi_data['cost_per_hire']:,.0f}",
            f"{roi_data['time_to_fill']} days",
            f"{roi_score}/10",
        ]
        alt_fill = _FILL_OFF_WHITE if idx % 2 == 0 else _FILL_WHITE
        row = _write_table_row(ws, row, values, alternate=(idx % 2 == 0))

        # Override ROI score cell styling
        roi_cell = ws.cell(row=row - 1, column=COL_START + 6)
        roi_cell.font = score_font
        roi_cell.fill = score_fill

    row += 1

    # ── Conversion Rate Assumptions ──
    row = _write_subsection_header(ws, row, "Conversion Rate Assumptions")

    assumption_headers = [
        "Channel Type",
        "App-to-Hire Rate",
        "Time to Fill Range",
        "Notes",
    ]
    row = _write_table_header(ws, row, assumption_headers)

    assumption_data = [
        ("Job Boards", "8-12%", "30-45 days", "High volume, broad reach"),
        ("Programmatic/DSP", "5-8%", "25-35 days", "Automated, cost-efficient"),
        ("Social Media", "3-6%", "35-50 days", "Brand awareness, passive candidates"),
        ("Niche/Specialty", "10-15%", "20-30 days", "Targeted, higher quality"),
        ("Aggregators/Regional", "6-10%", "30-45 days", "Geographic targeting"),
        ("Referrals", "15-25%", "15-25 days", "Highest conversion rate"),
        ("Career Sites", "8-12%", "25-40 days", "Direct applicants, lower cost"),
    ]

    for idx, (ch_type, rate, ttf_range, notes) in enumerate(assumption_data):
        values = [ch_type, rate, ttf_range, notes]
        alt_fill = _FILL_OFF_WHITE if idx % 2 == 0 else _FILL_WHITE
        row = _write_table_row(ws, row, values, alternate=(idx % 2 == 0))

    row += 1
    row = _write_footnote(
        ws,
        row,
        "Conversion rates are industry averages from SHRM, Appcast, and CEB research. "
        "Actual rates vary by role seniority, location, and employer brand strength.",
    )
    row = _write_footnote(
        ws,
        row,
        "ROI Score: 9-10 = Excellent, 7-8 = Good, 4-6 = Average, 1-3 = Below Average.",
    )

    row += 1
    _write_attribution_footer(ws, row)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN GENERATOR FUNCTION
# ═══════════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════════
# SHEET 6: Quality Intelligence (Gold Standard Gates)
# ═══════════════════════════════════════════════════════════════════════════════


def _build_sheet_quality_intelligence(
    ws, data: dict, gold_standard: dict[str, Any]
) -> None:
    """Build the Quality Intelligence worksheet from Gold Standard gate outputs.

    Renders 7 sections corresponding to the quality gates:
    1. City-level supply-demand data
    2. Security clearance segmentation (if applicable)
    3. Competitor mapping per city
    4. Difficulty classification per role
    5. Channel strategy (traditional vs non-traditional)
    6. Budget tier breakdowns
    7. Activation event calendar

    Args:
        ws: The openpyxl worksheet to populate.
        data: The full enriched data dict.
        gold_standard: The ``data["_gold_standard"]`` dict from apply_all_quality_gates.
    """
    ws.title = "Quality Intelligence"
    ws.sheet_properties.tabColor = SAPPHIRE

    # Column widths (B-H)
    _set_column_widths(ws, {1: 2, 2: 22, 3: 18, 4: 18, 5: 18, 6: 18, 7: 18, 8: 18})

    row = 2

    # ── Title banner ──
    row = _write_section_header(ws, row, "QUALITY INTELLIGENCE -- GOLD STANDARD GATES")
    row += 1

    client_name = data.get("client_name") or "Client"
    industry_label = data.get("industry_label") or (
        (data.get("industry") or "").replace("_", " ").title()
    )
    row = _write_footnote(
        ws,
        row,
        f"Gold Standard quality gate analysis for {client_name} | "
        f"Industry: {industry_label} | "
        f"Generated {datetime.date.today().strftime('%B %d, %Y')}",
    )
    row += 1

    # ── Section 1: City-Level Supply-Demand Data ──
    city_data: dict = gold_standard.get("city_level_data") or {}
    try:
        if city_data:
            row = _write_subsection_header(ws, row, "City-Level Supply-Demand Data")
            row = _write_table_header(
                ws,
                row,
                [
                    "City",
                    "Salary Multiplier",
                    "Estimated Salary",
                    "Hiring Difficulty",
                    "Supply Tier",
                    "COL Index",
                    "Salary Range",
                ],
            )
            for idx, (city_name, info) in enumerate(city_data.items()):
                row = _write_table_row(
                    ws,
                    row,
                    [
                        city_name,
                        f"{info.get('salary_multiplier', 1.0):.2f}x",
                        f"${info.get('estimated_salary', 0):,.0f}",
                        f"{info.get('hiring_difficulty', 0):.1f}/10",
                        str(info.get("supply_tier") or "balanced")
                        .replace("_", " ")
                        .title(),
                        f"{info.get('cost_of_living_index', 100):.1f}",
                        str(info.get("salary_range") or "N/A"),
                    ],
                    alternate=idx % 2 == 1,
                )
            row = _write_footnote(
                ws,
                row,
                "Salary multipliers relative to national average. "
                "Hiring difficulty: 1 (easy) to 10 (hardest).",
            )
            row += 1
    except Exception as exc:
        logger.error(
            "Quality Intelligence: city-level section failed: %s", exc, exc_info=True
        )
        row += 1

    # ── Section 2: Security Clearance Segmentation ──
    clearance: Optional[dict] = gold_standard.get("clearance_segmentation")
    try:
        if clearance:
            row = _write_subsection_header(ws, row, "Security Clearance Segmentation")

            primary = clearance.get("primary_clearance") or {}
            row = _write_kv_row(
                ws, row, "Defense Related", "Yes -- clearance requirements detected"
            )
            row = _write_kv_row(
                ws, row, "Primary Clearance", str(primary.get("level") or "N/A")
            )
            row = _write_kv_row(
                ws,
                row,
                "Detected Keywords",
                ", ".join(clearance.get("detected_keywords") or []),
            )
            row += 1

            # Clearance tiers table
            all_tiers: list = clearance.get("all_clearance_tiers") or []
            if all_tiers:
                row = _write_table_header(
                    ws,
                    row,
                    [
                        "Clearance Level",
                        "Salary Premium",
                        "Time-to-Fill (wks)",
                        "Pool Reduction",
                        "Budget Multiplier",
                        "Recommended Channels",
                    ],
                )
                for idx, tier in enumerate(all_tiers):
                    row = _write_table_row(
                        ws,
                        row,
                        [
                            str(tier.get("level") or ""),
                            f"+{tier.get('salary_premium_pct', 0)}%",
                            str(tier.get("time_to_fill_weeks") or ""),
                            f"{tier.get('candidate_pool_reduction_pct', 0)}%",
                            f"{tier.get('budget_multiplier', 1.0):.1f}x",
                            ", ".join(tier.get("channels") or []),
                        ],
                        alternate=idx % 2 == 1,
                    )

            # Recommendations
            recs: list = clearance.get("recommendations") or []
            for rec in recs:
                row = _write_kv_row(ws, row, "Recommendation", str(rec))
            row += 1
    except Exception as exc:
        logger.error(
            "Quality Intelligence: clearance section failed: %s", exc, exc_info=True
        )
        row += 1

    # ── Section 3: Competitor Mapping with Counter-Strategies ──
    competitor_map: dict = gold_standard.get("competitor_mapping") or {}
    try:
        if competitor_map:
            row = _write_subsection_header(
                ws, row, "Competitive Landscape & Counter-Strategies"
            )
            row = _write_table_header(
                ws,
                row,
                [
                    "City",
                    "Top Employers",
                    "Hiring Intensity",
                    "Est. Competing Postings",
                    "Why They Matter",
                    "Counter-Strategy",
                ],
            )
            client_name_qs = data.get("client_name") or "Client"
            industry_label_qs = data.get("industry_label") or (
                (data.get("industry") or "").replace("_", " ").title()
            )
            for idx, (city_name, info) in enumerate(competitor_map.items()):
                if city_name.startswith("_"):
                    continue  # skip internal keys like _national
                employers = info.get("top_employers") or []
                intensity = str(info.get("hiring_intensity") or "moderate").lower()
                est_postings = info.get("estimated_competing_postings") or "N/A"

                # Generate WHY each competitor group matters
                if intensity in ("high", "very_high"):
                    why_matter = (
                        f"High hiring volume in {city_name} -- "
                        f"these employers compete for the same {industry_label_qs} talent pool"
                    )
                elif intensity == "moderate":
                    why_matter = (
                        f"Active but not dominant -- opportunity to capture market share "
                        f"with targeted positioning in {city_name}"
                    )
                else:
                    why_matter = (
                        f"Lower competition in {city_name} -- favorable market for "
                        f"{client_name_qs}'s talent acquisition"
                    )

                # Generate counter-strategy
                if intensity in ("high", "very_high") and employers:
                    top_employer = employers[0] if employers else "competitors"
                    counter = (
                        f"Differentiate vs {top_employer}: emphasize career growth, "
                        f"culture, and work-life balance. "
                        f"Increase niche channel spend to find passive candidates."
                    )
                elif intensity == "moderate":
                    counter = (
                        f"Leverage speed-to-hire advantage. "
                        f"Target candidates frustrated with slow processes at larger firms."
                    )
                else:
                    counter = (
                        f"Capitalize on low competition with aggressive employer brand "
                        f"presence. Consider community events and local partnerships."
                    )

                row = _write_table_row(
                    ws,
                    row,
                    [
                        city_name,
                        ", ".join(employers[:4]),
                        intensity.title(),
                        str(est_postings),
                        why_matter[:100],
                        counter[:120],
                    ],
                    alternate=idx % 2 == 1,
                )

            # National competitors row
            national: dict = competitor_map.get("_national") or {}
            if national:
                national_employers = national.get("top_employers") or []
                row = _write_table_row(
                    ws,
                    row,
                    [
                        "National (All Markets)",
                        ", ".join(national_employers[:5]),
                        str(national.get("hiring_intensity") or "moderate").title(),
                        "",
                        "National competitors set salary and benefits benchmarks",
                        "Match or exceed top benefits; lead with mission and impact",
                    ],
                    fonts=[
                        _FONT_BODY_BOLD,
                        _FONT_BODY,
                        _FONT_BODY,
                        _FONT_BODY,
                        _FONT_BODY,
                        _FONT_BODY,
                    ],
                )
            row += 1
    except Exception as exc:
        logger.error(
            "Quality Intelligence: competitor section failed: %s", exc, exc_info=True
        )
        row += 1

    # ── Section 4: Difficulty Classification ──
    difficulty_framework: list = gold_standard.get("difficulty_framework") or []
    try:
        if difficulty_framework:
            row = _write_subsection_header(ws, row, "Role Difficulty Classification")
            row = _write_table_header(
                ws,
                row,
                [
                    "Role Title",
                    "Seniority Level",
                    "Difficulty (1-10)",
                    "Supply Level",
                    "Avg Time-to-Fill",
                    "Location Modifier",
                    "Budget Weight",
                    "Channel Emphasis",
                    "Description",
                ],
            )
            for idx, role_info in enumerate(difficulty_framework):
                loc_mod = role_info.get("location_modifier", 0.0)
                loc_name = role_info.get("location_matched") or ""
                loc_display = (
                    f"+{loc_mod:.1f} ({loc_name})"
                    if loc_mod > 0 and loc_name
                    else (
                        f"{loc_mod:.1f} ({loc_name})"
                        if loc_mod < 0 and loc_name
                        else "0 (baseline)"
                    )
                )
                supply_raw = str(role_info.get("supply_level") or "moderate")
                supply_display = supply_raw.replace("_", " ").title()
                row = _write_table_row(
                    ws,
                    row,
                    [
                        str(role_info.get("role_title") or ""),
                        str(role_info.get("seniority_level") or "mid").title(),
                        str(role_info.get("complexity_score") or ""),
                        supply_display,
                        f"{role_info.get('avg_time_to_fill_days', 0)} days",
                        loc_display,
                        f"{role_info.get('budget_weight', 1.0):.1f}x",
                        str(role_info.get("channel_emphasis") or "")
                        .replace("_", " ")
                        .title(),
                        str(role_info.get("description") or ""),
                    ],
                    alternate=idx % 2 == 1,
                )
            row += 1
    except Exception as exc:
        logger.error(
            "Quality Intelligence: difficulty section failed: %s", exc, exc_info=True
        )
        row += 1

    # ── Section 5: Channel Strategy ──
    channel_strategy: dict = gold_standard.get("channel_strategy") or {}
    try:
        if channel_strategy:
            row = _write_subsection_header(
                ws, row, "Channel Strategy -- Traditional vs Non-Traditional"
            )

            split = channel_strategy.get("recommended_split") or {}
            trad_pct = split.get("traditional_pct", 65)
            nontrad_pct = split.get("non_traditional_pct", 35)
            avg_complexity = channel_strategy.get("avg_role_complexity", 0)

            row = _write_kv_row(
                ws,
                row,
                "Recommended Split",
                f"{trad_pct}% Traditional / {nontrad_pct}% Non-Traditional",
            )
            row = _write_kv_row(ws, row, "Avg Role Complexity", f"{avg_complexity}/10")
            strategy_note = channel_strategy.get("strategy_note") or ""
            if strategy_note:
                row = _write_kv_row(ws, row, "Strategy Note", strategy_note)
            row += 1

            # Traditional channels
            trad_channels: list = channel_strategy.get("traditional_channels") or []
            if trad_channels:
                row = _write_table_header(
                    ws,
                    row,
                    ["Traditional Channel", "Type", "Reach", "Relevance Score"],
                    fill=_FILL_BLUE_LIGHT,
                )
                for idx, ch in enumerate(trad_channels):
                    row = _write_table_row(
                        ws,
                        row,
                        [
                            str(ch.get("name") or ""),
                            str(ch.get("type") or "").replace("_", " ").title(),
                            str(ch.get("reach") or "").title(),
                            str(ch.get("relevance_score") or ""),
                        ],
                        alternate=idx % 2 == 1,
                    )
                row += 1

            # Non-traditional channels
            nontrad_channels: list = (
                channel_strategy.get("non_traditional_channels") or []
            )
            if nontrad_channels:
                row = _write_table_header(
                    ws,
                    row,
                    ["Non-Traditional Channel", "Type", "Reach"],
                    fill=_FILL_BLUE_LIGHT,
                )
                for idx, ch in enumerate(nontrad_channels):
                    row = _write_table_row(
                        ws,
                        row,
                        [
                            str(ch.get("name") or ""),
                            str(ch.get("type") or "").replace("_", " ").title(),
                            str(ch.get("reach") or "").title(),
                        ],
                        alternate=idx % 2 == 1,
                    )
                row += 1
    except Exception as exc:
        logger.error(
            "Quality Intelligence: channel strategy section failed: %s",
            exc,
            exc_info=True,
        )
        row += 1

    # ── Section 6: Budget Tier Breakdowns ──
    budget_tiers: dict = gold_standard.get("budget_tiers") or {}
    try:
        if budget_tiers and "error" not in budget_tiers:
            row = _write_subsection_header(ws, row, "Multi-Tier Budget Breakdown")
            total = budget_tiers.get("total_budget", 0)
            row = _write_kv_row(ws, row, "Total Budget", f"${total:,.0f}")
            row += 1

            tier_breakdown: dict = budget_tiers.get("tier_breakdown") or {}
            row = _write_table_header(
                ws,
                row,
                ["Budget Tier", "Amount", "Percentage", "Description"],
            )
            for idx, (tier_key, tier_info) in enumerate(tier_breakdown.items()):
                tier_label = tier_key.replace("_", " ").title()
                row = _write_table_row(
                    ws,
                    row,
                    [
                        tier_label,
                        f"${tier_info.get('amount', 0):,.0f}",
                        f"{tier_info.get('pct', 0):.1f}%",
                        str(tier_info.get("description") or ""),
                    ],
                    alternate=idx % 2 == 1,
                )

                # Sub-allocations
                sub_alloc: dict = tier_info.get("sub_allocation") or {}
                if sub_alloc:
                    for sub_key, sub_amount in sub_alloc.items():
                        sub_label = f"  -- {sub_key.replace('_', ' ').title()}"
                        row = _write_table_row(
                            ws,
                            row,
                            [sub_label, f"${sub_amount:,.0f}", "", ""],
                            fonts=[_FONT_FOOTNOTE, _FONT_FOOTNOTE, None, None],
                        )
            row += 1

            # Budget recommendations
            recs: list = budget_tiers.get("recommendations") or []
            for rec in recs:
                row = _write_kv_row(ws, row, "Recommendation", str(rec))
            row += 1
    except Exception as exc:
        logger.error(
            "Quality Intelligence: budget tiers section failed: %s", exc, exc_info=True
        )
        row += 1

    # ── Section 7: Activation Event Calendar ──
    activation: dict = gold_standard.get("activation_calendar") or {}
    try:
        if activation:
            row = _write_subsection_header(ws, row, "Activation Event Calendar")
            start_month = activation.get("campaign_start_month", 0)
            if start_month:
                row = _write_kv_row(
                    ws,
                    row,
                    "Campaign Start",
                    datetime.date(2026, start_month, 1).strftime("%B %Y"),
                )
            phasing_note = activation.get("budget_phasing_note") or ""
            if phasing_note:
                row = _write_kv_row(ws, row, "Budget Phasing", phasing_note)
            row += 1

            timeline: list = activation.get("timeline") or []
            if timeline:
                row = _write_table_header(
                    ws,
                    row,
                    [
                        "Month",
                        "Season",
                        "Hiring Intensity",
                        "Budget Weight",
                        "Key Events",
                        "Recommendation",
                    ],
                )
                for idx, month_info in enumerate(timeline):
                    events = month_info.get("key_events") or []
                    row = _write_table_row(
                        ws,
                        row,
                        [
                            str(month_info.get("month_name") or ""),
                            str(month_info.get("season") or ""),
                            str(month_info.get("hiring_intensity") or "")
                            .replace("_", " ")
                            .title(),
                            f"{month_info.get('budget_weight', 1.0):.1f}x",
                            "; ".join(events),
                            str(month_info.get("recommendation") or ""),
                        ],
                        alternate=idx % 2 == 1,
                    )
                row += 1

            # Industry-specific events
            industry_events: list = activation.get("industry_events") or []
            if industry_events:
                row = _write_kv_row(
                    ws,
                    row,
                    "Industry Events",
                    "; ".join(industry_events),
                )
                row += 1
    except Exception as exc:
        logger.error(
            "Quality Intelligence: activation calendar section failed: %s",
            exc,
            exc_info=True,
        )
        row += 1

    # ── Attribution footer ──
    row += 1
    _write_attribution_footer(ws, row)


def generate_excel_v2(
    data: dict,
    research_mod=None,
    load_kb_fn=None,
    classify_tier_fn=None,
    fetch_logo_fn=None,
) -> bytes:
    """Generate a consolidated 5-sheet media plan Excel file.

    Args:
        data: The enriched data dict (same as generate_excel receives).
        research_mod: The research module for live data calls.
        load_kb_fn: Function to load knowledge base.
        classify_tier_fn: Function to classify role tiers.
        fetch_logo_fn: Function to fetch client logo.

    Returns:
        bytes: The Excel file as bytes.
    """
    # ── Input normalization (mirrors generate_excel for compatibility) ──
    if data.get("budget_range") and not data.get("budget"):
        data["budget"] = data["budget_range"]

    for key, default in [
        ("client_name", "Client"),
        ("company_name", "Client"),
        ("industry", "general_entry_level"),
        ("budget", "Not specified"),
        ("work_environment", "hybrid"),
    ]:
        if not data.get(key):
            data[key] = default

    # Normalize client name casing (preserves known brands)
    data["client_name"] = _proper_client_name(data["client_name"] or "Client")
    data["company_name"] = _proper_client_name(data["company_name"] or "Client")

    for key in ["locations", "roles", "target_roles", "campaign_goals", "competitors"]:
        val = data.get(key)
        if val is None:
            data[key] = []
        elif isinstance(val, str):
            data[key] = [val]

    # Normalize work_environment: frontend sends array, we need a string
    we = data.get("work_environment", "hybrid")
    if isinstance(we, list):
        data["work_environment"] = we[0] if we else "hybrid"
    elif not isinstance(we, str):
        data["work_environment"] = str(we) if we else "hybrid"

    # Normalize role titles
    roles = _get_roles(data)
    data["roles"] = roles
    data["target_roles"] = roles

    # Ensure tier data exists
    if not data.get("_role_tiers") and classify_tier_fn:
        role_tiers = {}
        for role in roles:
            try:
                role_tiers[role] = classify_tier_fn(role)
            except Exception:
                role_tiers[role] = {"tier": "Professional", "sourcing_strategy": ""}
        data["_role_tiers"] = role_tiers

        tier_groups = {}
        for role, tier_info in role_tiers.items():
            tier_name = tier_info.get("tier", "Professional")
            if tier_name not in tier_groups:
                tier_groups[tier_name] = {
                    "count": 0,
                    "roles": [],
                    "tier_info": tier_info,
                }
            tier_groups[tier_name]["count"] += 1
            tier_groups[tier_name]["roles"].append(role)
        data["_tier_groups"] = tier_groups

    # Ensure enriched/synthesized dicts exist
    if not data.get("_enriched"):
        data["_enriched"] = {}
    if not data.get("_synthesized"):
        data["_synthesized"] = {}
    if not data.get("_budget_allocation"):
        data["_budget_allocation"] = {}

    # ── Create workbook ──
    wb = Workbook()

    client_name = data.get("client_name", "Client")
    wb.properties.title = f"Recruitment Media Plan - {client_name}"
    wb.properties.creator = "Nova AI by Joveo"
    wb.properties.subject = f"AI-generated recruitment media plan for {client_name}"
    wb.properties.keywords = (
        f"recruitment media plan, "
        f"{data.get('industry') or ''.replace('_', ' ').title()}, "
        "job advertising"
    )
    wb.properties.description = (
        "Generated by Nova AI Media Plan Generator. "
        "Consolidated 5-sheet format with ROI projections."
    )
    wb.properties.category = "Recruitment Advertising"
    wb.properties.lastModifiedBy = "Nova AI by Joveo"
    # Bug #17 fix: Strip application metadata that leaks server tech (openpyxl version)
    wb.properties.application = "Nova AI Suite"
    wb.properties.appVersion = ""

    # ── Sheet 1: Executive Summary ──
    ws1 = wb.active  # Use the default first sheet
    try:
        _build_sheet_executive_summary(
            ws1,
            data,
            research_mod=research_mod,
            load_kb_fn=load_kb_fn,
            classify_tier_fn=classify_tier_fn,
        )
    except Exception as exc:
        logger.error("Executive Summary sheet failed: %s", exc, exc_info=True)
        # Critical sheet -- re-raise to fail the generation
        raise RuntimeError(f"Failed to build Executive Summary: {exc}") from exc

    # ── Sheet 2: Channels & Strategy ──
    ws2 = wb.create_sheet()
    try:
        _build_sheet_channels(
            ws2,
            data,
            research_mod=research_mod,
            load_kb_fn=load_kb_fn,
        )
    except Exception as exc:
        logger.error("Channel Strategy sheet failed: %s", exc, exc_info=True)
        # Critical sheet -- re-raise to fail the generation
        raise RuntimeError(f"Failed to build Channel Strategy: {exc}") from exc

    # ── Sheet 3: Market Intelligence ──
    ws3 = wb.create_sheet()
    try:
        _build_sheet_market_intelligence(
            ws3,
            data,
            research_mod=research_mod,
        )
    except Exception as exc:
        logger.error("Sheet 3 (Market Intelligence) failed: %s", exc, exc_info=True)
        ws3.title = "Market Intelligence"
        ws3.cell(
            row=2, column=2, value=f"Error generating Market Intelligence sheet: {exc}"
        ).font = _FONT_BODY

    # ── Sheet 4: Sources & Confidence ──
    ws4 = wb.create_sheet()
    try:
        _build_sheet_sources(ws4, data)
    except Exception as exc:
        logger.error("Sheet 4 (Sources & Confidence) failed: %s", exc, exc_info=True)
        ws4.title = "Sources & Confidence"
        ws4.cell(
            row=2, column=2, value=f"Error generating Sources sheet: {exc}"
        ).font = _FONT_BODY

    # ── Sheet 5: ROI Projections ──
    ws5 = wb.create_sheet()
    try:
        _build_sheet_roi_projections(ws5, data)
    except Exception as exc:
        logger.error("Sheet 5 (ROI Projections) failed: %s", exc, exc_info=True)
        ws5.title = "ROI Projections"
        ws5.cell(
            row=2, column=2, value=f"Error generating ROI Projections sheet: {exc}"
        ).font = _FONT_BODY

    # ── Sheet 6: Quality Intelligence (Gold Standard gates) ──
    gold_standard = data.get("_gold_standard") or {}
    if gold_standard:
        ws6 = wb.create_sheet()
        try:
            _build_sheet_quality_intelligence(ws6, data, gold_standard)
        except Exception as exc:
            logger.error(
                "Sheet 6 (Quality Intelligence) failed: %s", exc, exc_info=True
            )
            ws6.title = "Quality Intelligence"
            ws6.cell(
                row=2,
                column=2,
                value=f"Error generating Quality Intelligence sheet: {exc}",
            ).font = _FONT_BODY

    # ── Write to bytes ──
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output.getvalue()
