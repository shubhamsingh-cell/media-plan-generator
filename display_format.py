"""Single presentation layer for client-facing numbers, labels, and names.

Consolidates formatting logic that was previously duplicated (and drifting)
across excel_v2.py, ppt_generator.py, and app.py -- the shipped bugs this
fixes include raw ``snake_case`` channel keys leaking into workbook cells,
"$150.0K" trailing-zero artifacts, and an "18 months" duration turning into
17 months after a round-trip through a rounded weeks value.
"""

from __future__ import annotations

import math
import re
from typing import Any

# ---------------------------------------------------------------------------
# Channel display names
# ---------------------------------------------------------------------------
# Internal channel keys (from ppt_generator.INDUSTRY_ALLOC_PROFILES / CHANNEL_
# ALLOC and budget_engine.CHANNEL_QUALITY_SCORES / CHANNEL_NAME_TO_CATEGORY)
# mapped to client-facing display names.
CHANNEL_DISPLAY: dict[str, str] = {
    # Core plan channels (ppt_generator.CHANNEL_ALLOC / INDUSTRY_ALLOC_PROFILES)
    "niche_boards": "Niche / Industry Boards",
    "programmatic_dsp": "Programmatic (DSP)",
    "global_boards": "Global Job Boards",
    "social_media": "Social Media",
    "regional_boards": "Regional Job Boards",
    "employer_branding": "Employer Branding",
    "apac_regional": "APAC Regional",
    "emea_regional": "EMEA Regional",
    # Additional channel types referenced elsewhere in the codebase
    "direct_hiring": "Direct Hiring",
    "lead_generation": "Lead Generation",
    "referrals": "Referrals",
    "referral": "Referral Programs",
    "staffing_agency": "Staffing Agencies",
    "staffing": "Staffing Agencies",
    "search_engine": "Search / SEM",
    "search": "Search / SEM",
    "career_site": "Career Sites",
    "display_retargeting": "Display / Retargeting",
    "display": "Display / Banner",
    "events_jobfairs": "Recruitment Events & Job Fairs",
    "events": "Recruitment Events & Job Fairs",
    "internal_mobility": "Internal Mobility",
    "university": "University / Campus Recruiting",
    "job_board": "Job Boards",
    "programmatic": "Programmatic",
    "social": "Social Media",
    "email": "Email Marketing",
    "regional": "Regional & Local Boards",
}


# ---------------------------------------------------------------------------
# Acronym-preserving title case
# ---------------------------------------------------------------------------
# copy:both#5-family: plain ``str.title()`` clobbers domain acronyms embedded
# in snake_case KB keys/labels -- e.g. "cdl_drivers" -> "Cdl Drivers" instead
# of "CDL Drivers". This allowlist covers the acronyms actually seen in
# recruitment KB data (role/credential abbreviations); extend as new ones
# surface rather than special-casing individual strings at call sites.
ACRONYMS: set[str] = {
    "CDL", "RN", "LPN", "CNA", "HVAC", "CPA", "CPC", "CPH", "DSP", "ATS",
}


def smart_title(s: str) -> str:
    """Acronym-preserving title case for a snake_case or space-separated
    label. 'cdl_drivers' -> 'CDL Drivers' (not 'Cdl Drivers'); any word whose
    upper-cased form is in :data:`ACRONYMS` is rendered in full caps, every
    other word is capitalized normally."""
    if not s or not isinstance(s, str):
        return s or ""
    words = s.replace("_", " ").split()
    out = []
    for w in words:
        if w.upper() in ACRONYMS:
            out.append(w.upper())
        else:
            out.append(w[:1].upper() + w[1:].lower() if w else w)
    return " ".join(out)


def channel_label(key: str) -> str:
    """Client-facing channel name. Falls back to a title-cased version of the
    key so callers NEVER see a raw ``snake_case`` key on a workbook/deck."""
    if not key or not isinstance(key, str):
        return ""
    if key in CHANNEL_DISPLAY:
        return CHANNEL_DISPLAY[key]
    return key.replace("_", " ").title()


# ---------------------------------------------------------------------------
# Client / company name casing
# ---------------------------------------------------------------------------
def _cap_word(word: str) -> str:
    if not word:
        return word
    if word.islower():
        return word[0].upper() + word[1:]
    # Has internal capitals (eBay, McKinsey) or is an acronym (AMC, UPS) --
    # preserve as-is.
    return word


def client_display_name(raw: str | None) -> str:
    """Word-wise client name casing.

    - A word that is fully lowercase gets its first letter capitalized.
    - A word with internal capitals (eBay, McKinsey) or an acronym (AMC, UPS)
      is preserved as-is.
    - If EVERY word in the string is uppercase, the whole thing reads as raw
      shouty source data rather than real acronyms, so the whole string is
      title-cased instead ('MANPOWER - AMERIGAS' -> 'Manpower - Amerigas').
    """
    if not raw or not isinstance(raw, str):
        return ""
    collapsed = re.sub(r"\s+", " ", raw).strip()
    if not collapsed:
        return ""
    if collapsed.isupper():
        return " ".join(w.capitalize() for w in collapsed.split(" "))
    return " ".join(_cap_word(w) for w in collapsed.split(" "))


# ---------------------------------------------------------------------------
# Money / percent / count formatting
# ---------------------------------------------------------------------------
def fmt_money(x: float, compact: bool = False) -> str:
    """150000 -> '$150,000'; compact: '$150K'; 52500 compact -> '$52.5K'
    (trailing '.0' is always stripped -- never '$150.0K')."""
    try:
        val = float(x)
    except (TypeError, ValueError):
        return "$0"
    sign = "-" if val < 0 else ""
    val = abs(val)
    if compact:
        if val >= 1_000_000:
            scaled = val / 1_000_000
            body = f"{scaled:.1f}".rstrip("0").rstrip(".")
            return f"{sign}${body}M"
        if val >= 1_000:
            scaled = val / 1_000
            body = f"{scaled:.1f}".rstrip("0").rstrip(".")
            return f"{sign}${body}K"
        return f"{sign}${val:,.0f}"
    return f"{sign}${val:,.0f}"


def fmt_pct(x: float, decimals: int = 0, is_fraction: bool = False) -> str:
    """Format a percent. ``x`` is a 0-100 value by default; pass
    ``is_fraction=True`` when ``x`` is a 0-1 fraction instead. Never emits
    more than 2 decimal places regardless of ``decimals``."""
    try:
        val = float(x)
    except (TypeError, ValueError):
        return "0%"
    if is_fraction:
        val *= 100
    d = max(0, min(2, int(decimals)))
    return f"{val:.{d}f}%"


def _pluralize(word: str) -> str:
    if not word:
        return word
    lower = word.lower()
    if lower.endswith(("s", "x", "z", "ch", "sh")):
        return word + "es"
    if lower.endswith("y") and len(word) > 1 and lower[-2] not in "aeiou":
        return word[:-1] + "ies"
    return word + "s"


def fmt_count(n: int, noun: str) -> str:
    """'1 market', '6 markets' -- never emits '(s)'."""
    try:
        count = int(n)
    except (TypeError, ValueError):
        count = 0
    label = noun if count == 1 else _pluralize(noun)
    return f"{count} {label}"


def fmt_float(x: float, max_decimals: int = 2) -> str:
    """Format a float, stripping trailing zeros (and a trailing '.')."""
    try:
        val = float(x)
    except (TypeError, ValueError):
        return "0"
    d = max(0, int(max_decimals))
    s = f"{val:.{d}f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


# ---------------------------------------------------------------------------
# Monthly-to-total reconciliation (largest remainder method)
# ---------------------------------------------------------------------------
def reconcile_monthly_to_total(monthly: list[float], total: float) -> list[int]:
    """Round each monthly value to an int such that the ints sum EXACTLY to
    ``round(total)``, using the largest-remainder method (fewest possible
    per-bucket adjustments, ties broken by original list order)."""
    if not monthly:
        return []
    target = int(round(total))
    n = len(monthly)
    vals = [float(v) if isinstance(v, (int, float)) else 0.0 for v in monthly]
    floors = [math.floor(v) for v in vals]
    remainders = [v - f for v, f in zip(vals, floors)]
    result = list(floors)
    diff = target - sum(floors)

    if diff > 0:
        order = sorted(range(n), key=lambda i: (-remainders[i], i))
        idx = 0
        while diff > 0:
            result[order[idx % n]] += 1
            diff -= 1
            idx += 1
    elif diff < 0:
        order = sorted(range(n), key=lambda i: (remainders[i], i))
        idx = 0
        while diff < 0:
            result[order[idx % n]] -= 1
            diff += 1
            idx += 1

    return [int(v) for v in result]


# ---------------------------------------------------------------------------
# Duration <-> weeks (exact round-trip)
# ---------------------------------------------------------------------------
_WEEKS_RE = re.compile(r"~\s*(\d+)\s*week")
_NUM_RE = re.compile(r"[\d.]+")


def parse_duration_to_weeks(text: str | int | float) -> int:
    """'6 months' -> 26, '18 months'/'1.5 years' -> 78, '12 weeks' -> 12,
    a bare int/numeric-only string = weeks. Uses 52/12 weeks-per-month
    CONSISTENTLY with :func:`weeks_to_duration_label`.

    Labels produced by :func:`weeks_to_duration_label` embed the literal
    week count as "(~NN weeks)"; when present, that literal count is used
    directly so ``parse_duration_to_weeks(weeks_to_duration_label(w)) == w``
    for every ``w``, not just multiples of the 52/12 ratio.
    """
    if isinstance(text, (int, float)) and not isinstance(text, bool):
        return max(0, round(float(text)))
    if not isinstance(text, str):
        return 0
    s = text.strip().lower()
    if not s:
        return 0

    literal = _WEEKS_RE.search(s)
    if literal:
        return max(0, int(literal.group(1)))

    match = _NUM_RE.search(s)
    if not match:
        return 0
    num = float(match.group())

    if "year" in s:
        weeks = num * 52.0
    elif "month" in s:
        weeks = num * 52.0 / 12.0
    elif "week" in s:
        weeks = num
    elif "day" in s:
        weeks = num / 7.0
    else:
        weeks = num  # bare numeric string -- treat as weeks

    return max(0, round(weeks))


def weeks_to_duration_label(weeks: int) -> str:
    """Exact inverse framing of :func:`parse_duration_to_weeks`:
    26 -> '6 months (~26 weeks)', 78 -> '18 months (~78 weeks)'.
    <=13 weeks stays expressed in weeks."""
    w = max(0, int(weeks or 0))
    if w <= 13:
        return f"{w} weeks"

    months = round(w * 12.0 / 52.0)
    if months <= 24:
        return f"{months} months (~{w} weeks)"

    years = months / 12.0
    if abs(years - round(years)) < 0.05:
        yr = int(round(years))
        yr_txt = f"{yr} year" + ("s" if yr != 1 else "")
    else:
        yr_txt = f"{years:.1f} years"
    return f"{yr_txt} (~{w} weeks)"


# ---------------------------------------------------------------------------
# Hire goal parsing + gap statement
# ---------------------------------------------------------------------------
def parse_hire_goal(hire_volume: Any) -> int:
    """Parse the client's stated hiring GOAL to a comparable integer (low
    end). Ported verbatim from excel_v2.py:1665-1692 (``_parse_hire_goal``).

    ``hire_volume`` arrives as a free-text field: a bare int, "5000 hires",
    "50-100 hires", "5,000+", or "Not specified"/"TBD"/"". For a range we take
    the LOW end. Returns 0 when no numeric goal can be parsed, which callers
    treat as "no goal stated".
    """
    if isinstance(hire_volume, (int, float)):
        return max(0, int(hire_volume))
    if not isinstance(hire_volume, str):
        return 0
    text = hire_volume.strip().lower()
    if not text or text in ("not specified", "tbd", "n/a", "none", "unknown"):
        return 0
    nums = re.findall(r"\d[\d,]*", text)
    if not nums:
        return 0
    try:
        return max(0, int(nums[0].replace(",", "")))
    except ValueError:
        return 0


def goal_gap(projected_hires: int, goal: int, cost_per_hire: float) -> dict | None:
    """Honest gap statement between the client's stated hiring goal and the
    plan's projected hires. ``None`` when there is no goal to compare against
    (``goal <= 0``) or the plan already meets/exceeds it."""
    try:
        goal_i = int(goal)
    except (TypeError, ValueError):
        return None
    try:
        projected_i = int(projected_hires)
    except (TypeError, ValueError):
        projected_i = 0
    if goal_i <= 0 or projected_i >= goal_i:
        return None
    try:
        cph = float(cost_per_hire)
    except (TypeError, ValueError):
        cph = 0.0

    pct_of_goal = (projected_i / goal_i) * 100 if goal_i else 0.0
    additional_budget = (goal_i - projected_i) * cph
    return {
        "goal": goal_i,
        "projected": projected_i,
        "pct_of_goal": round(pct_of_goal, 1),
        "additional_budget": round(additional_budget, 2),
    }
