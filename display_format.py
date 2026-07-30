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
    "CDL",
    "RN",
    "LPN",
    "CNA",
    "HVAC",
    "CPA",
    "CPC",
    "CPH",
    "DSP",
    "ATS",
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
# Canonical campaign-duration resolution (single source of truth)
# ---------------------------------------------------------------------------
# Real shipped defect (Uber, brief campaign_duration="1-3 months"): the
# workbook's Executive Summary + 90-Day Forecast said "4 weeks" (a raw
# string re-parsed through parse_duration_to_weeks's generic 52/12 numeric
# rule, which reads "1-3 months" as just "1 month"), the deck's
# Implementation Timeline + the workbook's own Optimization Milestones said
# "Weeks 1-12" (app.py's phrase-ladder bucket for the SAME string), the
# deck's Next Steps slide said "1-3 months" (the raw brief string, never
# resolved at all), and the 90-Day Forecast window showed a fixed ~13-week
# date range -- four different derivations of one input. This is the S91
# drift bug class recurring: app.py's inline phrase ladder and this
# module's parse_duration_to_weeks were two independently maintained
# week-resolvers that could (and did) disagree, and a THIRD, separate
# duration-label formatter lived in app.py's own request handler
# (diverging from weeks_to_duration_label -- e.g. 80 weeks read back as
# "1.5 years (~18 months)" there but "18 months (~80 weeks)" here; the
# latter is the one every regression test in this repo actually asserts).
#
# Everything below is now the ONE place that maps a free-text
# campaign_duration to a week count (:func:`resolve_campaign_weeks`) and to
# the label shown anywhere in a bundle (:func:`resolve_campaign_duration_
# label`). app.py, excel_v2.py and ppt_generator.py all delegate here
# instead of re-deriving their own answer.

_UNBOUNDED_DURATION_LABEL = "Ongoing (no fixed end date)"
_UNBOUNDED_DURATION_EXACT = frozenset({"unbounded", "not specified", "tbd", "n/a"})

# Fixed marketing buckets the wizard's duration dropdown offers, each mapped
# to a deliberately "round" week count for phasing purposes -- NOT a strict
# 52/12 conversion of the phrase's own numbers (e.g. "6 months" -> 24 weeks
# here, a 4-weeks/month convention that matches every reference bundle and
# regression test in this repo, vs. parse_duration_to_weeks("6 months") ==
# 26, the general 52/12 free-text conversion used for values that AREN'T
# one of these dropdown options). Order matters: a phrase must be checked
# before a shorter phrase it could otherwise be mistaken for (e.g. "1-2
# year" before "2 year"). Literal single-month phrases ("1 month", "2
# month", "3 month") were deliberately removed from the "1-3 month" bucket
# so they no longer match here and instead fall through to the accurate
# 52/12 conversion in parse_duration_to_weeks below -- per Jesse Ofner's
# 2026-07-31 feedback that this bucket was silently expanding a requested
# "1 month" plan to ~84 days (12 weeks).
_DURATION_PHRASE_LADDER: tuple[tuple[tuple[str, ...], int], ...] = (
    (("2-5 year", "long-term", "long term"), 156),
    (("1-2 year", "2 year"), 80),
    (("6-12 month", "9 month", "12 month", "1 year"), 48),
    (("3-6 month", "4 month", "5 month", "6 month"), 24),
    (("1-3 month",), 12),
    (("ongoing",), 52),
)


def is_unbounded_duration(duration: Any) -> bool:
    """True for the wizard's "Ongoing" duration option (any case/whitespace
    variant or synonym), or an empty/not-specified/TBD placeholder -- an
    open-ended campaign with no fixed end date, as opposed to a fixed
    length like "6 months". Single source of truth for a check that used
    to be duplicated verbatim in excel_v2.py and ppt_generator.py (and
    would otherwise need to recognise both the raw wizard value "Ongoing"
    and the canonical label :data:`_UNBOUNDED_DURATION_LABEL` this module
    produces for it)."""
    s = str(duration or "").strip().lower()
    if not s or s in _UNBOUNDED_DURATION_EXACT:
        return True
    return "ongoing" in s


def resolve_campaign_weeks(duration_str: Any) -> int:
    """THE single source of truth mapping a free-text campaign_duration
    string to a week count.

    Merges what used to be two independently maintained ladders: app.py's
    inline phrase ladder for the wizard's fixed dropdown buckets, and this
    module's own :func:`parse_duration_to_weeks` for everything else. Every
    caller must resolve campaign_weeks through this one function so the
    same duration string can never produce two different week counts
    depending on which module happened to parse it.

    Resolution order: a fixed marketing-bucket phrase first (dropdown
    option -- includes "ongoing" -> 52, an annual-cycle approximation used
    for PHASING math only, never for the duration label -- see
    :func:`resolve_campaign_duration_label`), then
    :func:`parse_duration_to_weeks` for anything else (explicit "N
    weeks"/"N months"/"N years", bare numerics). Unparseable/unrecognized
    input defaults to 12 weeks, matching the legacy ladder's own default.
    """
    s = str(duration_str or "").strip().lower()
    for phrases, weeks in _DURATION_PHRASE_LADDER:
        if any(p in s for p in phrases):
            return weeks
    weeks = parse_duration_to_weeks(s)
    return weeks if weeks > 0 else 12


def resolve_campaign_duration_label(data: dict) -> str:
    """THE single source of truth for the campaign-duration STRING shown
    anywhere in a bundle (Executive Summary, 90-Day Forecast, deck Next
    Steps/Implementation Timeline...). Every surface must render THIS
    value rather than re-deriving its own wording from the raw brief
    string, so the bundle can never state its duration two different ways.

    Preference order:
      1. An explicitly "Ongoing"/"unbounded" raw duration always wins --
         regardless of any numeric campaign_weeks already computed for
         phasing purposes -- so an open-ended campaign never reads as a
         specific fixed length (the prod defect this guards against:
         "Ongoing" silently resolving to "1 year (~12 months)").
      2. ``data["campaign_duration_canonical"]`` when already resolved
         (set once, upstream -- e.g. by app.py from campaign_weeks).
      3. Derived from ``data["campaign_weeks"]`` when present.
      4. Derived by resolving the raw ``campaign_duration``/``timeline``
         string through :func:`resolve_campaign_weeks`.
      5. The raw string itself, or "Not specified".
    """
    raw = str(data.get("campaign_duration") or data.get("timeline") or "").strip()
    raw_lower = raw.lower()
    if raw_lower == "unbounded" or "ongoing" in raw_lower:
        return _UNBOUNDED_DURATION_LABEL

    canonical = data.get("campaign_duration_canonical")
    if isinstance(canonical, str) and canonical.strip():
        return canonical.strip()

    weeks = data.get("campaign_weeks")
    try:
        weeks_int = int(weeks) if weeks else 0
    except (TypeError, ValueError):
        weeks_int = 0
    if weeks_int > 0:
        return weeks_to_duration_label(weeks_int)

    if not raw_lower or raw_lower in ("not specified", "tbd", "n/a"):
        return "Not specified"

    derived = resolve_campaign_weeks(raw)
    if derived > 0:
        return weeks_to_duration_label(derived)
    return raw or "Not specified"


def scale_week_phases(total_weeks: int, num_phases: int) -> list[tuple[int, int]]:
    """Partition weeks 1..``total_weeks`` into ``num_phases`` contiguous,
    non-overlapping (start, end) week ranges whose FINAL phase always ends
    EXACTLY at ``total_weeks`` -- so a "Week N-M" phase/milestone table
    built from this can never contradict the campaign's own canonical
    duration (the real shipped defect this guards against: a fixed
    "Week 1-12" milestones table rendered unchanged regardless of whether
    the campaign was 4 weeks or 78).

    A campaign shorter than ``num_phases`` weeks legitimately compresses --
    several phases can share the same single week (mirrors how
    ppt_generator's own Implementation Timeline slide already collapses
    its phases for a <=12-week campaign, e.g. "Weeks 4-4") -- rather than
    ever spilling a phase past the campaign's real end.
    """
    total = max(1, int(total_weeks or 0))
    n = max(1, int(num_phases or 1))
    phases: list[tuple[int, int]] = []
    prev_end = 0
    for i in range(1, n + 1):
        raw_end = round(i * total / n)
        start = min(prev_end + 1, total)
        end = max(raw_end, start)
        end = min(end, total)
        phases.append((start, end))
        prev_end = end
    # Rounding can leave the last phase short of `total` (e.g. total=52,
    # n=6 rounds to .../44-52 already, but not every (total, n) pair does)
    # -- pin it exactly so the bundle's own phased timeline always agrees
    # with the canonical campaign_weeks to the week.
    last_start, _ = phases[-1]
    phases[-1] = (last_start, total)
    return phases


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
