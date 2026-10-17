"""Enterprise hourly hiring pack -- extra plan sections for high-volume hourly clients.

Opt-in (wizard toggle ``enterprise_pack``).  When the toggle is off nothing in
this module runs and every output is unchanged.

When on, the plan gains five workbook sheets and three deck appendix slides,
all built from ONE model (:func:`build_model`) so the workbook and the deck
can never disagree:

* **Weekly Pacing** -- fiscal weeks (configurable week start), spend by
  calendar weight, the budget ladder (minimum test -> recommended -> surge)
  with an expected apply-start range and its basis, Green / Amber / Red
  bands, pause / reallocate rules, and a site x week split.
* **Publisher Tiers & Gates** -- Tier 1 / 2 / 3 launch plan from the plan's
  funded publishers (client exclusions and availability respected),
  per-publisher minimum spend, switch triggers and weekly performance gates.
* **Tracking & Measurement** -- UTM / campaign-ID convention, system of
  record, reconciliation rule and worksheet, launch checklist.
* **Feasibility Check** -- pass / check / fail / confirm rows.
* **Job Ad Quality** -- checklist with the flags the plan can detect.

No benchmark number is invented here.  Every threshold, factor and trigger
default is an EDITABLE ASSUMPTION (a shaded input cell labelled "default");
the sheets use real Excel formulas so editing a cell recalculates the sheet.
Expected apply starts come from the plan's own cost-per-application range and
are omitted, with a note, when the plan carries none.

The module is deliberately self-contained (new sheets, new slides, one small
request sanitizer) so the existing sheet and slide builders are untouched.
"""

from __future__ import annotations

import datetime
import logging
import math
import re
from typing import Any, Dict, List, Optional, Tuple

import demand_planning

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Request layer (wizard payload -> validated, capped fields)
# ---------------------------------------------------------------------------
REQUEST_KEYS: Tuple[str, ...] = (
    "enterprise_pack",
    "enterprise_week_start",
    "enterprise_system_of_record",
    "enterprise_request_lock_day",
    "enterprise_setup_sla_days",
    "site_goals",
)
WEEKDAYS: Tuple[str, ...] = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)
MAX_SYSTEM_OF_RECORD_CHARS = 120
MAX_SETUP_SLA_DAYS = 30
DEFAULT_WEEK_START = "sunday"
DEFAULT_SYSTEM_OF_RECORD = "client ATS / analytics — confirm"
DEFAULT_WEEKS = 13  # a quarter, used only when the plan states no length
MAX_WEEKS = 110  # a 24-month window plus its boundary weeks
MAX_SITES = 40
MAX_PUBLISHERS = 40

# Sheet titles (<= 31 chars).
SHEET_PACING = "Weekly Pacing"
SHEET_TIERS = "Publisher Tiers & Gates"
SHEET_TRACKING = "Tracking & Measurement"
SHEET_FEASIBILITY = "Feasibility Check"
SHEET_JOBAD = "Job Ad Quality"
# Wave 2 (demand-first): present only when a hiring goal was stated.
SHEET_GOALPLAN = "Goal Plan"
SHEET_TITLES: Tuple[str, ...] = (
    SHEET_PACING,
    SHEET_TIERS,
    SHEET_TRACKING,
    SHEET_FEASIBILITY,
    SHEET_JOBAD,
)

# Editable defaults.  None of these is a benchmark: each is surfaced as a
# shaded input cell labelled "default -- set to the client's SLA / plan".
DEFAULT_MIN_TEST_FACTOR = 0.5
DEFAULT_SURGE_FACTOR = 1.5
DEFAULT_GREEN = 0.90
DEFAULT_AMBER = 0.85
DEFAULT_TIER1_CUM = 0.60
DEFAULT_TIER2_CUM = 0.90
DEFAULT_MIN_MARKET_BOARDS = 3
DEFAULT_RECON_TOLERANCE = 0.10
DEFAULT_FIX_WINDOW_HOURS = 48
DEFAULT_REALLOCATE_DAYS = 7
DEFAULT_PROMOTE_PCT = 1.0
DEFAULT_PROMOTE_DAYS = 7
DEFAULT_PAUSE_DAYS = 14

DEFAULT_LABEL = "default — set to the client's SLA"
DEFAULT_PLAN_LABEL = "default — adjust to the client's plan"


def enabled(data: Any) -> bool:
    """True when the plan asked for the enterprise hourly hiring pack."""
    if not isinstance(data, dict):
        return False
    val = data.get("enterprise_pack")
    if isinstance(val, str):
        return val.strip().lower() in ("true", "1", "yes", "on")
    if isinstance(val, (bool, int)):
        return bool(val)
    return False


def _weekday_or_none(raw: Any) -> Optional[str]:
    text = str(raw or "").strip().lower()
    return text if text in WEEKDAYS else None


def request_problem(data: Any) -> Tuple[str, str]:
    """("", "") when the enterprise request fields are acceptable, else
    (code, message).  Mirrors the other request-boundary checks in app.py."""
    if not isinstance(data, dict):
        return "", ""
    ws = data.get("enterprise_week_start")
    if ws not in (None, "") and _weekday_or_none(ws) is None:
        return "week_start", "enterprise_week_start must be a day of the week."
    ld = data.get("enterprise_request_lock_day")
    if ld not in (None, "") and _weekday_or_none(ld) is None:
        return "lock_day", "enterprise_request_lock_day must be a day of the week."
    sla = data.get("enterprise_setup_sla_days")
    if sla not in (None, ""):
        try:
            if isinstance(sla, bool):
                raise ValueError
            n = int(float(sla))
        except (TypeError, ValueError):
            return "sla", "enterprise_setup_sla_days must be a whole number of days."
        if not 0 <= n <= MAX_SETUP_SLA_DAYS:
            return (
                "sla",
                f"enterprise_setup_sla_days must be 0 to {MAX_SETUP_SLA_DAYS} days.",
            )
    sor = data.get("enterprise_system_of_record")
    if sor not in (None, ""):
        if not isinstance(sor, str):
            return "type", "enterprise_system_of_record must be text."
        if len(sor.strip()) > MAX_SYSTEM_OF_RECORD_CHARS:
            return (
                "too_long",
                "The system of record is too long "
                f"(max {MAX_SYSTEM_OF_RECORD_CHARS} characters).",
            )
    sg_code, sg_msg = demand_planning.site_goals_problem(data.get("site_goals"))
    if sg_code:
        return "site_goals", sg_msg
    return "", ""


_PROBLEM_FIELDS = {
    "site_goals": "site_goals",
    "week_start": "enterprise_week_start",
    "lock_day": "enterprise_request_lock_day",
    "sla": "enterprise_setup_sla_days",
    "too_long": "enterprise_system_of_record",
    "type": "enterprise_system_of_record",
}


def problem_field(code: str) -> str:
    """The request field a :func:`request_problem` code is about."""
    return _PROBLEM_FIELDS.get(code, "enterprise_pack")


def normalize_request(data: Any) -> None:
    """Rewrite the enterprise request fields to their canonical form in
    place.  When the toggle is off every enterprise field is dropped, so a
    stray field can never change a standard plan."""
    if not isinstance(data, dict):
        return
    if not enabled(data):
        for key in REQUEST_KEYS:
            data.pop(key, None)
        return
    data["enterprise_pack"] = True
    data["enterprise_week_start"] = (
        _weekday_or_none(data.get("enterprise_week_start")) or DEFAULT_WEEK_START
    )
    data["enterprise_system_of_record"] = str(
        data.get("enterprise_system_of_record") or ""
    ).strip()[:MAX_SYSTEM_OF_RECORD_CHARS]
    data["enterprise_request_lock_day"] = (
        _weekday_or_none(data.get("enterprise_request_lock_day")) or ""
    )
    sla = data.get("enterprise_setup_sla_days")
    try:
        n = (
            int(float(sla))
            if sla not in (None, "") and not isinstance(sla, bool)
            else None
        )
    except (TypeError, ValueError):
        n = None
    data["enterprise_setup_sla_days"] = (
        n if n is not None and 0 <= n <= MAX_SETUP_SLA_DAYS else ""
    )
    goals = demand_planning.clean_site_goals(data.get("site_goals"))
    if goals:
        data["site_goals"] = goals
    else:
        data.pop("site_goals", None)


def settings(data: Any) -> Dict[str, Any]:
    """Effective settings (defaults applied) for an enabled plan."""
    d = data if isinstance(data, dict) else {}
    sla = d.get("enterprise_setup_sla_days")
    try:
        sla_n: Optional[int] = (
            int(float(sla))
            if sla not in (None, "") and not isinstance(sla, bool)
            else None
        )
    except (TypeError, ValueError):
        sla_n = None
    return {
        "week_start": _weekday_or_none(d.get("enterprise_week_start"))
        or DEFAULT_WEEK_START,
        "system_of_record": str(d.get("enterprise_system_of_record") or "").strip(),
        "lock_day": _weekday_or_none(d.get("enterprise_request_lock_day")) or "",
        "setup_sla_days": sla_n,
    }


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------
def _today() -> datetime.date:
    return datetime.date.today()


def _xl_round(value: float, digits: int = 0) -> float:
    """Excel's ROUND: halves round away from zero (Python's round() takes them
    to the even neighbour, so a cached value could differ from the formula)."""
    from decimal import ROUND_HALF_UP, Decimal

    return float(
        Decimal(repr(float(value))).quantize(
            Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP
        )
    )


def _num(value: Any) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return 0.0
    return out if math.isfinite(out) else 0.0


def _slug(text: Any, limit: int = 24) -> str:
    out = re.sub(r"[^a-z0-9]+", "-", str(text or "").lower()).strip("-")
    return out[:limit].strip("-")


def _weekday_name(idx: int) -> str:
    return WEEKDAYS[idx % 7].capitalize()


def _plan_start(data: dict, today: datetime.date) -> datetime.date:
    try:
        month = int(float(data.get("campaign_start_month") or 0))
    except (TypeError, ValueError):
        month = 0
    if not 1 <= month <= 12:
        month = today.month
    year = today.year if month >= today.month else today.year + 1
    return datetime.date(year, month, 1)


def _calendar_weights(data: dict) -> Tuple[Dict[int, float], bool]:
    """month number -> budget weight from the plan's activation calendar."""
    try:
        import gold_standard

        timeline = gold_standard.build_activation_calendar(data).get("timeline") or []
        weights = {
            int(m["month"]): _num(m.get("budget_weight")) or 1.0
            for m in timeline
            if isinstance(m, dict) and m.get("month")
        }
        return weights, bool(weights)
    except Exception as exc:  # noqa: BLE001 - weights fall back to flat
        logger.debug("enterprise_pack: calendar weights unavailable: %s", exc)
        return {}, False


def _campaign_window(data: dict, start: datetime.date) -> Tuple[datetime.date, str]:
    """(last day, basis) -- the real campaign window from the plan's own
    helpers: whole months via ``campaign_window_end``, else the carried
    weeks, else a default quarter."""
    try:
        import gold_standard

        end = gold_standard.campaign_window_end(data, start)
        if end is not None:
            return end, "stated campaign length"
        n = int(gold_standard._plan_campaign_weeks(data))
        if n > 0:
            return start + datetime.timedelta(days=n * 7 - 1), "stated campaign length"
    except Exception as exc:  # noqa: BLE001
        logger.debug("enterprise_pack: campaign window unavailable: %s", exc)
    return start + datetime.timedelta(days=DEFAULT_WEEKS * 7 - 1), "default"


def _fiscal_weeks(
    start: datetime.date,
    end: datetime.date,
    week_start_idx: int,
    month_weights: Dict[int, float],
) -> Tuple[List[Dict[str, Any]], bool]:
    """Fiscal weeks covering ``start``..``end`` exactly: week 1 is the week
    CONTAINING the start, the last week the one containing the end.  A week
    that is only partly inside the window is flagged and weighted by its
    active days (each day carries its month's calendar weight)."""
    first = start - datetime.timedelta(days=(start.weekday() - week_start_idx) % 7)
    weeks: List[Dict[str, Any]] = []
    wstart = first
    truncated = False
    while wstart <= end:
        if len(weeks) >= MAX_WEEKS:
            truncated = True
            break
        days = [wstart + datetime.timedelta(days=k) for k in range(7)]
        active = [d for d in days if start <= d <= end]
        weight = sum(month_weights.get(d.month, 1.0) for d in active) / 7.0
        weeks.append(
            {
                "n": len(weeks) + 1,
                "start": wstart,
                "end": wstart + datetime.timedelta(days=6),
                "days": len(active),
                "partial": len(active) < 7,
                "weight": round(weight, 2),
            }
        )
        wstart += datetime.timedelta(days=7)
    return weeks, truncated


def _locations(data: dict) -> List[str]:
    raw = data.get("locations") or []
    if isinstance(raw, (str, dict)):
        raw = [raw]
    out: List[str] = []
    seen: set = set()
    for loc in raw:
        if isinstance(loc, dict):
            parts = [
                str(loc.get(k) or "").strip() for k in ("city", "state", "country")
            ]
            text = ", ".join(p for p in parts[:2] if p) or parts[2]
        else:
            text = str(loc or "").strip()
        key = re.sub(r"[^a-z0-9]", "", text.lower())
        if text and key not in seen:
            seen.add(key)
            out.append(text)
    return out


def _city_key(text: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(text or "").split(",")[0].lower())


def _exact_shares(weights: List[float]) -> List[float]:
    """Weights normalised to shares at 4 decimals that add to EXACTLY 1: the
    last share is one minus the others (rounding each of three equal shares
    gave 0.9999 and a $15 gap against the plan)."""
    total = sum(weights) or 1.0
    shares = [round(w / total, 4) for w in weights[:-1]]
    return shares + [round(1.0 - sum(shares), 4)]


def _site_shares(data: dict, sites: List[str]) -> Tuple[List[float], str]:
    """Each site's fair share of spend: the plan's own location weights
    (budget_engine ``location_adjustments``) normalised to 1, else equal."""
    n = len(sites)
    if n == 0:
        return [], "none"
    adj_raw = (data.get("_budget_allocation") or {}).get("location_adjustments") or {}
    adj = {_city_key(k): _num(v) for k, v in adj_raw.items() if _num(v) > 0}
    weights = [adj.get(_city_key(s)) for s in sites]
    known = [w for w in weights if w]
    if not known:
        fallback = [1.0] * n
        default_basis = "equal shares (fair share)"
    else:
        fill = sum(known) / len(known)
        fallback = [w if w else fill for w in weights]
        default_basis = "plan location weights"
    # Per-site hiring goals (wave 2) replace the default weights: a site's
    # fair share is its share of the goals.  No goals (or none matching a
    # site) leaves the default allocation untouched.
    goals = site_goals(data)
    if goals:
        gw, gbasis = demand_planning.site_weights(
            sites, goals, total_goal=_hire_goal(data), fallback=fallback
        )
        if gw:
            total = sum(gw)
            shares = [round(w / total, 4) for w in gw]
            # the rounding remainder goes to the largest site, never to a
            # site whose goal is zero
            top = max(range(len(shares)), key=lambda i: gw[i])
            shares[top] = round(shares[top] + (1.0 - sum(shares)), 4)
            return shares, gbasis
    return _exact_shares(fallback), default_basis


def _funded_channels(data: dict) -> Tuple[List[Dict[str, Any]], int, float]:
    """(publishers, dropped_count, dropped_budget).

    Funded publishers/channels, largest first, after client exclusions and
    availability (publisher_filter) -- the same list every surface uses.  A
    publisher is judged per plan market: one that runs in none of the plan's
    countries is dropped (its budget is reported, never shown under a blank
    name); one that runs in only some keeps ``markets`` naming where."""
    ba = data.get("_budget_allocation") or {}
    alloc = ba.get("channel_allocations") or {}
    if not isinstance(alloc, dict):
        return [], 0, 0.0
    try:
        import publisher_filter as pf
    except ImportError:  # pragma: no cover
        pf = None
    countries = pf.plan_countries(data) if pf is not None else []
    rows: List[Dict[str, Any]] = []
    dropped = 0
    dropped_budget = 0.0
    for name, info in alloc.items():
        if not isinstance(info, dict):
            continue
        dollars = _num(info.get("dollar_amount"))
        if dollars <= 0:
            continue
        if pf is not None and pf.is_excluded(name, data):
            continue
        runs: List[str] = []
        if pf is not None and countries:
            runs = [
                c
                for c in countries
                if pf.platform_runs_in(name, c) and not pf.is_excluded(name, data, c)
            ]
            if not runs:
                dropped += 1
                dropped_budget += dollars
                continue
        display = (
            pf.display_name(name, runs[0] if runs else None)
            if pf is not None
            else str(name)
        )
        display = str(display or name).strip()
        if not display:
            continue
        rows.append(
            {
                "name": display,
                "raw_name": str(name),
                "category": str(info.get("category") or ""),
                "budget": dollars,
                "cpa": _num(info.get("cpa")),
                "applications": _num(info.get("projected_applications")),
                "markets": runs,
                "all_markets": (not countries) or len(runs) == len(countries),
            }
        )
    rows.sort(key=lambda r: -r["budget"])
    return rows[:MAX_PUBLISHERS], dropped, dropped_budget


def _cpa_range(channels: List[Dict[str, Any]]) -> Optional[Tuple[float, float]]:
    cpas = [c["cpa"] for c in channels if c["cpa"] > 0]
    if len(cpas) < 2:
        return None
    lo, hi = min(cpas), max(cpas)
    return (lo, hi) if hi > lo * 1.0001 else None


def _cpa_info(
    channels: List[Dict[str, Any]], ba: dict
) -> Tuple[Optional[Tuple[float, float]], str]:
    """(range, basis).  The funded publishers' own cost-per-application range
    when it has spread; else the plan's blended cost per application (a point:
    low == high); else (None, "none") and apply starts are not estimated."""
    rng = _cpa_range(channels)
    if rng:
        return rng, "channels"
    blended = _num((ba.get("total_projected") or {}).get("cost_per_application"))
    if blended > 0:
        return (blended, blended), "blended"
    single = [c["cpa"] for c in channels if c["cpa"] > 0]
    if single:
        return (single[0], single[0]), "blended"
    return None, "none"


def site_goals(data: Any) -> Dict[str, int]:
    """The plan's per-site hiring goals ({} unless the pack is on and goals
    were given).  Reading is defensive: a hand-built plan dict is cleaned the
    same way the request layer cleans a payload."""
    if not enabled(data):
        return {}
    raw = data.get("site_goals")
    if demand_planning.site_goals_problem(raw)[0]:
        return {}
    return demand_planning.clean_site_goals(raw)


def _hire_goal(data: dict) -> int:
    goals = site_goals(data)
    if goals:
        # per-site goals ARE the plan's goal: their total is what the plan is
        # graded against
        return sum(goals.values())
    meta = (data.get("_budget_allocation") or {}).get("metadata") or {}
    openings = int(_num(meta.get("total_openings")))
    if openings > 0:
        return openings
    m = re.search(r"(\d[\d,]*)", str(data.get("hire_volume") or ""))
    return int(m.group(1).replace(",", "")) if m else 0


def stated_goal(data: Any) -> int:
    """The plan's hiring goal as the client stated it: the total of the
    per-site goals when any were given, else the hire volume / exact hires
    field (the SAME parse the deck's and workbook's goal-gap line use).  0 =
    no goal stated -- never the engine's projected-hires fallback."""
    goals = site_goals(data)
    if goals:
        return sum(goals.values())
    if not isinstance(data, dict):
        return 0
    try:
        import display_format

        return int(display_format.parse_hire_goal(data.get("hire_volume")))
    except (ImportError, TypeError, ValueError):
        return 0


def compute_goal_plan(
    data: dict,
    projector: Any,
    *,
    budget: float,
    symbol: str = "$",
    currency_code: str = "USD",
) -> Optional[Dict[str, Any]]:
    """The goal plan (:func:`demand_planning.build_goal_plan`) for an enabled
    plan with a stated goal, else None.  ``projector(b)`` is project_plan at
    budget ``b`` with the caller's own inputs, so the plan and the headline
    ranges can never disagree.  Per-site rows use the same shares as the
    Weekly Pacing site x week split."""
    if not enabled(data):
        return None
    goal = stated_goal(data)
    if goal <= 0:
        return None
    goals = site_goals(data)
    sites = _locations(data)
    shares, _basis = _site_shares(data, sites) if len(sites) >= 2 or goals else ([], "")
    plan = demand_planning.build_goal_plan(
        goal=goal,
        budget=budget,
        projector=projector,
        weeks=demand_planning.campaign_weeks(data),
        site_goals=goals,
        sites=sites if shares else [],
        shares=shares,
        symbol=symbol,
        currency_code=currency_code,
    )
    if plan.get("ok"):
        volume = str(data.get("hire_volume") or "")
        if not goals and re.search(r"\d\s*(?:-|\u2013|to)\s*\d|\d\s*\+", volume):
            plan.setdefault("notes", []).append(
                f"The hire volume was a range ({volume.strip()}): the plan is built "
                f"to its low end ({plan['goal']:,}). Enter an exact number to plan "
                "to a precise goal."
            )
        stated = 0
        if goals:
            try:
                import display_format

                stated = int(display_format.parse_hire_goal(data.get("hire_volume")))
            except (ImportError, TypeError, ValueError):
                stated = 0
        if goals and stated > 0 and stated != sum(goals.values()):
            plan.setdefault("notes", []).append(
                f"Site goals total {sum(goals.values()):,}; the plan is built to "
                "the site goals, not the hire volume field."
            )
    return plan


def _compliance_flags(data: dict) -> List[str]:
    flags: List[str] = []
    try:
        import gold_standard

        clr = gold_standard.detect_clearance_requirements(data)
        if isinstance(clr, dict) and clr.get("primary_clearance"):
            flags.append(
                "Security clearance requirement detected for this plan's roles."
            )
    except Exception as exc:  # noqa: BLE001
        logger.debug("enterprise_pack: clearance check unavailable: %s", exc)
    return flags


def _markets(data: dict, channels: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One row per plan market (country): sites in it and the publishers
    available to it (funded publishers that run there + local boards from the
    benchmark dataset, after exclusions)."""
    try:
        import publisher_filter as pf
    except ImportError:  # pragma: no cover
        return []
    groups: Dict[str, Dict[str, Any]] = {}
    for site in _locations(data):
        country = None
        try:
            import plan_geo

            if plan_geo.location_is_us(site) is True:
                country = "United States"
        except Exception:  # noqa: BLE001
            country = None
        if country is None:
            country = pf.detect_country(site) or "United States"
        country = pf.canonical_country(country) or country
        g = groups.setdefault(country, {"country": country, "sites": []})
        g["sites"].append(site)
    out: List[Dict[str, Any]] = []
    for country, g in groups.items():
        names: List[str] = [
            c["name"] for c in channels if pf.platform_runs_in(c["raw_name"], country)
        ]
        if country != "United States":
            local = pf.clean_names(pf.dataset_platform_names(country), data, country)
            have = {pf.normalize(n) for n in names}
            names += [n for n in local if pf.normalize(n) not in have]
        out.append(
            {
                "country": country,
                "sites": g["sites"],
                "boards": len(names),
                "board_names": names[:8],
            }
        )
    return out


_TITLE_JARGON_RE = re.compile(
    r"(\b(?:L\d|lvl\s*\d|level\s*\d|tier\s*\d|grade\s*\d)\b|\b(?:sr|jr)\.?\s|\s(?:ii|iii|iv)\b|[/()_]|#)",
    re.IGNORECASE,
)
_CLAIM_WORDS: Tuple[str, ...] = (
    "sign-on bonus",
    "signing bonus",
    "sign on bonus",
    "tuition",
    "guaranteed",
    "immediate start",
    "same-day pay",
    "same day pay",
    "free ",
    "health insurance",
    "benefits from day",
    "no experience",
    "flexible schedule",
    "remote",
)


def _brief_text(data: dict) -> str:
    return " ".join(
        str(data.get(k) or "") for k in ("use_case", "call_transcript", "notes")
    )


def _roles(data: dict) -> List[str]:
    out: List[str] = []
    for r in data.get("roles") or data.get("target_roles") or []:
        title = r.get("title", r.get("role", "")) if isinstance(r, dict) else r
        title = str(title or "").strip()
        if title and title not in out:
            out.append(title)
    return out[:30]


def build_model(data: dict, today: Optional[datetime.date] = None) -> Dict[str, Any]:
    """Everything the workbook sheets and the deck slides print, as plain
    data.  Pure: no workbook or slide code, no network."""
    today = today or _today()
    cfg = settings(data)
    ba = data.get("_budget_allocation") or {}
    channels, dropped_count, dropped_budget = _funded_channels(data)
    total = _num((ba.get("metadata") or {}).get("total_budget")) or sum(
        c["budget"] for c in channels
    )

    ws_idx = WEEKDAYS.index(cfg["week_start"])
    start = _plan_start(data, today)
    end, weeks_basis = _campaign_window(data, start)
    month_weights, weights_ok = _calendar_weights(data)
    weeks, weeks_truncated = _fiscal_weeks(start, end, ws_idx, month_weights)
    first_week = weeks[0]["start"]

    sites_all = _locations(data)
    shares, share_basis = _site_shares(data, sites_all)
    if len(sites_all) > MAX_SITES:
        keep = MAX_SITES - 1
        sites = sites_all[:keep] + [f"Other sites ({len(sites_all) - keep})"]
        shares = shares[:keep] + [round(1.0 - sum(shares[:keep]), 4)]
    else:
        sites = sites_all

    markets = _markets(data, channels)
    cpa, cpa_basis = _cpa_info(channels, ba)
    tp = ba.get("total_projected") or {}
    return {
        "settings": cfg,
        "start": start,
        "first_week": first_week,
        "end": end,
        "weeks": weeks,
        "weeks_truncated": weeks_truncated,
        # Active days / 7: the divisor for every "per week" average (a 2-week
        # campaign that starts mid-week has 3 week rows but 2.0 weeks).
        "effective_weeks": sum(w["days"] for w in weeks) / 7.0,
        "weeks_basis": weeks_basis,
        "weights_from_calendar": weights_ok,
        "total_budget": total,
        "channels": channels,
        "dropped_count": dropped_count,
        "dropped_budget": dropped_budget,
        "excluded": _excluded_names(data),
        "cpa_range": cpa,
        "cpa_basis": cpa_basis,
        "sites": sites,
        "site_count": len(sites_all),
        "shares": shares,
        "share_basis": share_basis,
        "markets": markets,
        "site_country": {site: m["country"] for m in markets for site in m["sites"]},
        "compliance_flags": _compliance_flags(data),
        "hire_goal": _hire_goal(data),
        "hires_low": _num(tp.get("hires_low")) or _num(tp.get("hires")),
        "hires_high": _num(tp.get("hires_high")) or _num(tp.get("hires")),
        "plan_slug": _slug(
            data.get("client_name") or data.get("company_name") or "plan"
        )
        or "plan",
        "roles": _roles(data),
        "brief_claims": sorted(
            {w.strip() for w in _CLAIM_WORDS if w in _brief_text(data).lower()}
        ),
        "jargon_titles": [
            t for t in _roles(data) if _TITLE_JARGON_RE.search(t) or len(t) > 60
        ],
        "pay_in_brief": bool(
            re.search(
                r"[$£€]\s?\d|\d\s?(?:/\s?h(?:ou)?r|per hour|an hour)",
                _brief_text(data),
                re.I,
            )
        ),
        "countries": _countries(data),
    }


def _excluded_names(data: dict) -> List[str]:
    try:
        import publisher_filter as pf

        return pf.user_excluded(data)
    except ImportError:  # pragma: no cover
        return []


def _countries(data: dict) -> List[str]:
    try:
        import publisher_filter as pf

        return pf.plan_countries(data)
    except ImportError:  # pragma: no cover
        return []


def tier_for(
    cum_before: float, t1: float = DEFAULT_TIER1_CUM, t2: float = DEFAULT_TIER2_CUM
) -> str:
    """Tier from the cumulative budget share ahead of a publisher (same rule
    as the workbook's live formula)."""
    return "Tier 1" if cum_before < t1 else "Tier 2" if cum_before < t2 else "Tier 3"


def tiered_publishers(model: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Funded publishers with share, cumulative-before and default tier."""
    chans = model["channels"]
    total = sum(c["budget"] for c in chans) or 1.0
    out: List[Dict[str, Any]] = []
    cum = 0.0
    for c in chans:
        share = c["budget"] / total
        out.append({**c, "share": share, "cum_before": cum, "tier": tier_for(cum)})
        cum += share
    return out


def format_range(lo: float, hi: float) -> str:
    if round(lo) == round(hi):
        return f"{lo:,.0f}"
    return f"{lo:,.0f}–{hi:,.0f}"


def ladder(model: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The three budget-ladder steps with defaults applied."""
    weeks = model["weeks"]
    total = model["total_budget"]
    n = model["effective_weeks"] or 1.0
    cpa = model["cpa_range"]
    rows = []
    for label, factor, basis in (
        (
            "Minimum test",
            DEFAULT_MIN_TEST_FACTOR,
            "Smallest weekly spend that still yields a readable signal.",
        ),
        (
            "Recommended weekly",
            1.0,
            "The plan's budget distributed by calendar weight.",
        ),
        (
            "Surge",
            DEFAULT_SURGE_FACTOR,
            "Weeks with a hiring deadline or a missed pace.",
        ),
    ):
        camp = total * factor
        row = {
            "step": label,
            "factor": factor,
            "weekly": camp / n,
            "campaign": camp,
            "basis": basis,
            "starts_low": None,
            "starts_high": None,
        }
        if cpa:
            row["starts_low"] = camp / cpa[1]
            row["starts_high"] = camp / cpa[0]
        rows.append(row)
    return rows


def feasibility_summary(model: Dict[str, Any]) -> Dict[str, Any]:
    """Default-assumption feasibility statuses (the workbook computes the
    same rows with live formulas)."""
    rows = feasibility_rows(model)
    counts = {"PASS": 0, "CHECK": 0, "FAIL": 0, "CONFIRM": 0}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    return {"rows": rows, "counts": counts}


def feasibility_rows(model: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The feasibility rows with their default-assumption status."""
    rows: List[Dict[str, Any]] = []
    for m in model["markets"]:
        ok = m["boards"] >= DEFAULT_MIN_MARKET_BOARDS
        rows.append(
            {
                "kind": "supply",
                "check": f"Supply in market: {m['country']}",
                "status": "PASS" if ok else "CHECK",
                "evidence": f"{m['boards']} publishers available across {len(m['sites'])} site(s)",
                "market": m,
            }
        )
    if not model["markets"]:
        rows.append(
            {
                "kind": "supply_none",
                "check": "Supply in market",
                "status": "CONFIRM",
                "evidence": "The plan names no location.",
            }
        )
    if model["excluded"]:
        rows.append(
            {
                "kind": "excluded",
                "check": "Publishers excluded at client request",
                "status": "PASS" if model["channels"] else "FAIL",
                "evidence": f"{len(model['excluded'])} excluded; {len(model['channels'])} funded publishers remain",
            }
        )
    flags = model["compliance_flags"]
    rows.append(
        {
            "kind": "compliance",
            "check": "Compliance flags computed for this plan",
            "status": "CHECK" if flags else "CONFIRM",
            "evidence": (
                " ".join(flags)
                if flags
                else "No compliance flags were computed; confirm local pay-transparency, consent and language rules."
            ),
        }
    )
    rows.append(
        {
            "kind": "ats",
            "check": "ATS redirect and tracking support",
            "status": "CONFIRM",
            "evidence": "Confirm the client's apply flow accepts a redirect and passes UTM parameters.",
        }
    )
    goal = model["hire_goal"]
    if goal > 0 and model["hires_high"] > 0:
        lo, hi = model["hires_low"], model["hires_high"]
        status = "PASS" if lo >= goal else "CHECK" if hi >= goal else "FAIL"
        evidence = f"Goal {goal:,} hires; plan projects {lo:,.0f}–{hi:,.0f}"
    else:
        status, evidence = (
            "CONFIRM",
            "No hiring goal or projection to compare; enter the goal.",
        )
    rows.append(
        {
            "kind": "scale",
            "check": "Scale vs target",
            "status": status,
            "evidence": evidence,
        }
    )
    cfg = model["settings"]
    rows.append(
        {
            "kind": "lead_times",
            "check": "Lead times (request lock day, setup SLA)",
            "status": (
                "PASS"
                if cfg["lock_day"] and cfg["setup_sla_days"] is not None
                else "CONFIRM"
            ),
            "evidence": (
                "Request lock day and setup SLA captured."
                if cfg["lock_day"] and cfg["setup_sla_days"] is not None
                else "Request lock day and/or setup SLA not provided."
            ),
        }
    )
    sor = cfg["system_of_record"]
    rows.append(
        {
            "kind": "sor",
            "check": "System of record for applies",
            "status": "PASS" if sor else "CONFIRM",
            "evidence": sor or "Not named; confirm which system counts applies.",
        }
    )
    return rows


# ---------------------------------------------------------------------------
# Workbook sheets
# ---------------------------------------------------------------------------
_CACHE_ATTR = "_nova_formula_cached_values"
_STATUS_COLORS = {"GREEN": "22C55E", "AMBER": "F59E0B", "RED": "EF4444"}


class _Sheet:
    """Thin cursor over a worksheet: styled writes, shaded input cells, and
    formulas that also record their value so previews that do not recalculate
    (Quick Look, Drive, Slack) still show numbers."""

    def __init__(self, ws: Any, x: Any) -> None:
        from openpyxl.styles import Font, PatternFill

        self.ws = ws
        self.x = x
        self.input_fill = PatternFill(
            start_color="FEF3C7", end_color="FEF3C7", fill_type="solid"
        )
        self.input_font = Font(name=x.FONT_BODY_NAME, bold=True, size=10, color=x.NAVY)
        cache = getattr(ws, _CACHE_ATTR, None)
        if cache is None:
            cache = {}
            setattr(ws, _CACHE_ATTR, cache)
        self.cache = cache

    # -- generic writes -----------------------------------------------------
    def put(
        self,
        row: int,
        col: int,
        value: Any,
        *,
        font: Any = None,
        fill: Any = None,
        fmt: Optional[str] = None,
        align: Any = None,
        border: bool = True,
    ) -> Any:
        x = self.x
        cell = self.ws.cell(row=row, column=col, value=value)
        cell.font = font or x._FONT_BODY
        cell.fill = fill or x._FILL_WHITE
        cell.alignment = align or x._ALIGN_WRAP
        if border:
            cell.border = x._BORDER_THIN
        if fmt:
            cell.number_format = fmt
        return cell

    def inp(self, row: int, col: int, value: Any, fmt: Optional[str] = None) -> Any:
        """A shaded, editable input cell."""
        return self.put(
            row,
            col,
            value,
            font=self.input_font,
            fill=self.input_fill,
            fmt=fmt,
            align=self.x._ALIGN_CENTER,
        )

    def formula(
        self,
        row: int,
        col: int,
        formula: str,
        value: Any,
        *,
        fmt: Optional[str] = None,
        font: Any = None,
        fill: Any = None,
        align: Any = None,
        as_input: bool = False,
    ) -> Any:
        """Write ``formula`` and remember ``value`` as its cached result."""
        if as_input:
            cell = self.inp(row, col, formula, fmt)
        else:
            cell = self.put(
                row,
                col,
                formula,
                fmt=fmt,
                font=font,
                fill=fill,
                align=align or self.x._ALIGN_CENTER,
            )
        if isinstance(value, bool) or value is None:
            value = ""
        if isinstance(value, float) and not math.isfinite(value):
            value = 0.0
        self.cache[cell.coordinate] = value
        return cell

    def merged(
        self,
        row: int,
        c1: int,
        c2: int,
        value: Any,
        *,
        font: Any = None,
        fill: Any = None,
        height: Optional[float] = None,
    ) -> Any:
        x = self.x
        self.ws.merge_cells(start_row=row, start_column=c1, end_row=row, end_column=c2)
        cell = self.put(row, c1, value, font=font, fill=fill)
        for c in range(c1 + 1, c2 + 1):
            self.ws.cell(row=row, column=c).border = x._BORDER_THIN
        if height:
            self.ws.row_dimensions[row].height = height
        elif isinstance(value, str):
            width = sum(
                self.ws.column_dimensions[_col(c)].width or 8.43
                for c in range(c1, c2 + 1)
            )
            lines = sum(
                max(1, math.ceil(len(p) / max(10.0, width * 1.05)))
                for p in value.split("\n")
            )
            if lines > 1:
                self.ws.row_dimensions[row].height = round(lines * 13.5 + 3, 1)
        return cell

    def header(self, row: int, headers: List[str], col: int = 2) -> int:
        x = self.x
        for i, h in enumerate(headers):
            c = self.ws.cell(row=row, column=col + i, value=h)
            c.font = x._FONT_TABLE_HEADER
            c.fill = x._FILL_SAPPHIRE
            c.alignment = x._ALIGN_CENTER
            c.border = x._BORDER_THIN
        self.ws.row_dimensions[row].height = 32
        return row + 1

    def section(self, row: int, title: str, last_col: int = 8) -> int:
        x = self.x
        self.ws.merge_cells(
            start_row=row, start_column=2, end_row=row, end_column=last_col
        )
        c = self.ws.cell(row=row, column=2, value=title.upper())
        c.font = x._FONT_SECTION
        c.fill = x._FILL_NAVY
        c.alignment = x._ALIGN_LEFT
        self.ws.row_dimensions[row].height = 28
        return row + 1

    def sub(self, row: int, title: str, last_col: int = 8) -> int:
        x = self.x
        self.ws.merge_cells(
            start_row=row, start_column=2, end_row=row, end_column=last_col
        )
        c = self.ws.cell(row=row, column=2, value=title)
        c.font = x._FONT_SUBSECTION
        c.alignment = x._ALIGN_LEFT
        c.border = x._BORDER_BOTTOM_SAPPHIRE
        self.ws.row_dimensions[row].height = 22
        return row + 1

    def note(self, row: int, text: str, last_col: int = 8) -> int:
        x = self.x
        self.ws.merge_cells(
            start_row=row, start_column=2, end_row=row, end_column=last_col
        )
        c = self.ws.cell(row=row, column=2, value=text)
        c.font = x._FONT_FOOTNOTE
        c.alignment = x._ALIGN_LEFT
        width = sum(
            self.ws.column_dimensions[_col(k)].width or 8.43
            for k in range(2, last_col + 1)
        )
        lines = sum(
            max(1, math.ceil(len(p) / max(10.0, width * 1.1))) for p in text.split("\n")
        )
        if lines > 1:
            self.ws.row_dimensions[row].height = round(lines * 12.5 + 2, 1)
        return row + 1

    def widths(self, widths: Dict[int, float]) -> None:
        for col, w in widths.items():
            self.ws.column_dimensions[_col(col)].width = w

    def status_rules(self, ref: str) -> None:
        """Green / amber / red conditional fills on a status range."""
        from openpyxl.formatting.rule import CellIsRule
        from openpyxl.styles import PatternFill

        for word, hexcode in _STATUS_COLORS.items():
            self.ws.conditional_formatting.add(
                ref,
                CellIsRule(
                    operator="equal",
                    formula=[f'"{word}"'],
                    fill=PatternFill(
                        start_color=hexcode, end_color=hexcode, fill_type="solid"
                    ),
                ),
            )

    def verdict_rules(self, ref: str) -> None:
        from openpyxl.formatting.rule import CellIsRule
        from openpyxl.styles import PatternFill

        palette = {
            "PASS": "BBF7D0",
            "CHECK": "FDE68A",
            "FAIL": "FECACA",
            "CONFIRM": "DDD6FE",
        }
        for word, hexcode in palette.items():
            self.ws.conditional_formatting.add(
                ref,
                CellIsRule(
                    operator="equal",
                    formula=[f'"{word}"'],
                    fill=PatternFill(
                        start_color=hexcode, end_color=hexcode, fill_type="solid"
                    ),
                ),
            )

    def dropdown(self, ref: str, options: List[str]) -> None:
        from openpyxl.worksheet.datavalidation import DataValidation

        dv = DataValidation(
            type="list", formula1='"' + ",".join(options) + '"', allow_blank=True
        )
        self.ws.add_data_validation(dv)
        dv.add(ref)


def _col(n: int) -> str:
    from openpyxl.utils import get_column_letter

    return get_column_letter(n)


def _a(col: int, row: int, absolute: bool = True) -> str:
    """A1 reference, absolute by default."""
    return f"${_col(col)}${row}" if absolute else f"{_col(col)}{row}"


def _xref(sheet: str, col: int, row: int) -> str:
    return f"'{sheet}'!{_a(col, row)}"


def _date_label(d: datetime.date) -> str:
    return f"{d.strftime('%a')} {d.day} {d.strftime('%b')}"


def _title_block(sh: _Sheet, title: str, purpose: str, last_col: int = 8) -> int:
    row = sh.section(2, title, last_col)
    row = sh.note(row, purpose, last_col)
    row = sh.note(
        row,
        "Shaded cells are editable assumptions: change one and the sheet recalculates. "
        "A cell labelled 'default' is a starting point, not a benchmark — set it to the client's own SLA.",
        last_col,
    )
    return row + 1


# -- Weekly Pacing -----------------------------------------------------------
def _build_pacing(ws: Any, data: dict, model: Dict[str, Any], x: Any) -> Dict[str, Any]:
    sh = _Sheet(ws, x)
    ws.title = SHEET_PACING
    cpa = model["cpa_range"]
    weeks = model["weeks"]
    n = len(weeks)
    eff = model["effective_weeks"] or 1.0
    ncol = 12 if cpa else 10
    sh.widths(
        {
            1: 3,
            2: 34,
            3: 14,
            4: 16,
            5: 16,
            6: 16,
            7: 16,
            8: 14,
            9: 14,
            10: 14,
            11: 16,
            12: 14,
        }
    )
    cfg = model["settings"]
    cur_fmt = x._usd0_fmt()
    cpa_fmt = x._usd2_fmt()
    pct = x.FMT_PCT1
    ints = x.FMT_INT

    row = _title_block(
        sh,
        "Weekly Pacing",
        "Spend by fiscal week for enterprise hourly hiring: the budget ladder (minimum test, "
        "recommended, surge), the expected apply-start range with its basis, and the status "
        "bands the weekly review uses.",
    )
    wd = _weekday_name(WEEKDAYS.index(cfg["week_start"]))
    end_wd = _weekday_name(WEEKDAYS.index(cfg["week_start"]) - 1)
    partial = [w for w in weeks if w["partial"]]
    row = (
        sh.note(
            row - 1,
            f"Fiscal week: {wd} to {end_wd}. Week 1 is the fiscal week that contains the campaign start "
            f"({model['start'].strftime('%d %b %Y')}); the last week contains the campaign end "
            f"({model['end'].strftime('%d %b %Y')}). Campaign length: {n} fiscal weeks ("
            + (
                "from the plan's stated length"
                if model["weeks_basis"] != "default"
                else f"no length stated, {DEFAULT_WEEKS}-week default"
            )
            + "). "
            + (
                f"{len(partial)} week(s) are only partly inside the campaign: they are flagged "
                "'partial' and weighted by their active days. "
                if partial
                else "Every week is a whole week. "
            )
            + "Calendar weights come from the plan's activation calendar"
            + (
                "."
                if model["weights_from_calendar"]
                else " (unavailable, flat weights used)."
            )
            + (
                f" Only the first {MAX_WEEKS} weeks are shown."
                if model["weeks_truncated"]
                else ""
            )
            + (
                ""
                if model["weeks_basis"] != "default"
                else " Set the weeks below to the real window."
            ),
        )
        + 1
    )

    # ---- assumptions ----
    row = sh.sub(row, "Assumptions (editable)")
    row = sh.header(row, ["Assumption", "Value", "Basis"], 2)
    ws.merge_cells(start_row=row - 1, start_column=4, end_row=row - 1, end_column=8)
    refs: Dict[str, Any] = {}

    def assume(
        key: str, label: str, value: Any, fmt: Optional[str], basis: str
    ) -> None:
        nonlocal row
        sh.put(row, 2, label, font=x._FONT_BODY_BOLD)
        sh.inp(row, 3, value, fmt)
        sh.merged(row, 4, 8, basis)
        refs[key] = (3, row)
        row += 1

    assume(
        "budget",
        "Planned campaign budget",
        model["total_budget"],
        cur_fmt,
        "From the plan's budget allocation; change it to re-spread every week below.",
    )
    assume(
        "min_factor",
        "Minimum-test factor (x recommended)",
        DEFAULT_MIN_TEST_FACTOR,
        pct,
        f"{DEFAULT_PLAN_LABEL}. The least weekly spend that still gives a readable signal.",
    )
    assume(
        "surge_factor",
        "Surge factor (x recommended)",
        DEFAULT_SURGE_FACTOR,
        pct,
        f"{DEFAULT_PLAN_LABEL}. Use in a deadline week or to recover a missed pace.",
    )
    if cpa:
        assume(
            "cpa_low",
            "Cost per apply start, low",
            round(cpa[0], 2),
            cpa_fmt,
            (
                "Lowest cost per application across the plan's funded publishers (apply starts are estimated from applications)."
                if model["cpa_basis"] == "channels"
                else "The plan's blended cost per application (one figure, so low and high are equal); overwrite to model a range."
            ),
        )
        assume(
            "cpa_high",
            "Cost per apply start, high",
            round(cpa[1], 2),
            cpa_fmt,
            (
                "Highest cost per application across the plan's funded publishers."
                if model["cpa_basis"] == "channels"
                else "Same blended figure; set a higher cost to widen the range."
            ),
        )
    assume(
        "green",
        "Green threshold (% of weekly goal)",
        DEFAULT_GREEN,
        pct,
        f"{DEFAULT_LABEL}. At or above this the week is Green.",
    )
    assume(
        "amber",
        "Amber threshold (% of weekly goal)",
        DEFAULT_AMBER,
        pct,
        f"{DEFAULT_LABEL}. At or above this (and below Green) the week is Amber; below it, Red.",
    )
    # campaign weeks: computed
    weeks_row = row
    sh.put(row, 2, "Campaign weeks (active days / 7)", font=x._FONT_BODY_BOLD)
    row += 1  # value written after the table exists
    row += 1

    # ---- ladder ----
    ladder_hdr_row = row
    row = sh.sub(row, "Budget ladder")
    ladder_headers = ["Step", "Factor", "Avg weekly spend", "Campaign spend"]
    if cpa:
        ladder_headers += [
            "Apply starts / week, low",
            "Apply starts / week, high",
            "Apply starts, campaign low",
            "Apply starts, campaign high",
        ]
    row = sh.header(row, ladder_headers, 2)
    ladder_first = row
    row += 3
    if cpa:
        row = sh.note(
            row,
            "Basis: apply starts = spend / cost per apply start (low and high from the assumptions above). "
            "Applications are the closest modelled proxy for apply starts; confirm against the system of record after week 1.",
        )
    else:
        row = sh.note(
            row,
            "Expected apply starts are omitted: the plan carries no cost-per-application figure to derive them from. "
            "Status bands still work: enter the client's weekly goal in the weekly table.",
        )
    row += 1

    # ---- legend ----
    row = sh.sub(
        row,
        (
            "Status legend (weekly goal = midpoint of the expected apply-start range)"
            if cpa
            else "Status legend (weekly goal = the client's goal, entered in the weekly table)"
        ),
    )
    row = sh.header(row, ["Status", "From (% of goal)", "To (% of goal)", "Action"], 2)
    ws.merge_cells(start_row=row - 1, start_column=5, end_row=row - 1, end_column=8)
    legend_first = row
    row += 3
    row = sh.sub(
        row,
        "Pause and reallocate rules (defaults — edit the thresholds above and the days below)",
    )
    rules_row = row
    rules = [
        "Daily check: compare each publisher's apply starts to its share of the weekly goal. Two consecutive RED days: stop adding spend to that publisher.",
        "Weekly check: a RED week means pause the lowest-performing publisher in the week and reallocate its budget to Tier 1 publishers that are GREEN.",
        "An AMBER week: hold budget, fix the cause (tracking, job-ad quality, site capacity) and review again before moving money.",
        "Never reallocate before tracking is validated: a publisher that looks RED because applies are not recorded is a tracking fault, not a performance fault.",
        "Surge: move to the surge level only for a named deadline week, and step back to recommended once the goal is met.",
    ]
    for r in rules:
        sh.merged(row, 2, 8, r)
        row += 1
    row += 1

    # ---- weekly table ----
    row = sh.sub(row, "Weekly pacing", ncol)
    headers = [
        "Week",
        "Calendar weight",
        "Recommended spend",
        "Minimum-test spend",
        "Surge spend",
        "Cumulative (recommended)",
    ]
    if cpa:
        headers += ["Apply starts, low", "Apply starts, high", "Weekly goal"]
    else:
        headers += ["Weekly goal (enter)"]
    headers += ["Actual apply starts (enter)", "Status", "Active days"]
    row = sh.header(row, headers, 2)
    goal_c = 10 if cpa else 8
    act_c, stat_c = goal_c + 1, goal_c + 2
    gl, al = _col(goal_c), _col(act_c)
    first = row
    last = row + n - 1
    total_row = last + 1
    bud = _a(*refs["budget"])
    minf = _a(*refs["min_factor"])
    surf = _a(*refs["surge_factor"])
    green = _a(*refs["green"])
    amber = _a(*refs["amber"])
    wsum = sum(w["weight"] for w in weeks) or 1.0
    budget = model["total_budget"]
    cum = 0.0
    tot = {"rec": 0.0, "min": 0.0, "surge": 0.0, "lo": 0.0, "hi": 0.0, "goal": 0.0}
    for i, w in enumerate(weeks):
        r = first + i
        alt = x._FILL_BLUE_PALE if i % 2 else x._FILL_WHITE
        sh.put(
            r,
            2,
            f"W{w['n']:02d}  {_date_label(w['start'])} – {_date_label(w['end'])}"
            + (f" (partial: {w['days']} of 7 days)" if w["partial"] else ""),
            fill=alt,
        )
        sh.inp(r, 3, round(w["weight"], 2), "0.00")
        rec = budget * w["weight"] / wsum
        sh.formula(
            r, 4, f"={bud}*C{r}/SUM($C${first}:$C${last})", rec, fmt=cur_fmt, fill=alt
        )
        sh.formula(
            r, 5, f"=D{r}*{minf}", rec * DEFAULT_MIN_TEST_FACTOR, fmt=cur_fmt, fill=alt
        )
        sh.formula(
            r, 6, f"=D{r}*{surf}", rec * DEFAULT_SURGE_FACTOR, fmt=cur_fmt, fill=alt
        )
        cum += rec
        sh.formula(r, 7, f"=SUM($D${first}:D{r})", cum, fmt=cur_fmt, fill=alt)
        tot["rec"] += rec
        tot["min"] += rec * DEFAULT_MIN_TEST_FACTOR
        tot["surge"] += rec * DEFAULT_SURGE_FACTOR
        if cpa:
            clo, chi = _a(*refs["cpa_low"]), _a(*refs["cpa_high"])
            lo, hi = rec / round(cpa[1], 2), rec / round(cpa[0], 2)
            sh.formula(r, 8, f"=D{r}/{chi}", lo, fmt=ints, fill=alt)
            sh.formula(r, 9, f"=D{r}/{clo}", hi, fmt=ints, fill=alt)
            sh.formula(r, 10, f"=(H{r}+I{r})/2", (lo + hi) / 2, fmt=ints, fill=alt)
            tot["lo"] += lo
            tot["hi"] += hi
            tot["goal"] += (lo + hi) / 2
        else:
            sh.inp(r, goal_c, None, ints)
        sh.inp(r, act_c, None, ints)
        sh.formula(
            r,
            stat_c,
            f'=IF(OR({al}{r}="",{gl}{r}="",{gl}{r}<=0),"",IF({al}{r}/{gl}{r}>={green},"GREEN",IF({al}{r}/{gl}{r}>={amber},"AMBER","RED")))',
            "",
        )
    # totals
    sh.put(total_row, 2, "Total", font=x._FONT_BODY_BOLD, fill=x._FILL_OFF_WHITE)
    sh.put(total_row, 3, None, fill=x._FILL_OFF_WHITE)
    for col, key, fmt in (
        (4, "rec", cur_fmt),
        (5, "min", cur_fmt),
        (6, "surge", cur_fmt),
    ):
        sh.formula(
            total_row,
            col,
            f"=SUM({_col(col)}{first}:{_col(col)}{last})",
            tot[key],
            fmt=fmt,
            font=x._FONT_BODY_BOLD,
            fill=x._FILL_OFF_WHITE,
        )
    sh.put(total_row, 7, None, fill=x._FILL_OFF_WHITE)
    if cpa:
        for col, key in ((8, "lo"), (9, "hi"), (10, "goal")):
            sh.formula(
                total_row,
                col,
                f"=SUM({_col(col)}{first}:{_col(col)}{last})",
                tot[key],
                fmt=ints,
                font=x._FONT_BODY_BOLD,
                fill=x._FILL_OFF_WHITE,
            )
    else:
        sh.formula(
            total_row,
            goal_c,
            f"=SUM({gl}{first}:{gl}{last})",
            0,
            fmt=ints,
            font=x._FONT_BODY_BOLD,
            fill=x._FILL_OFF_WHITE,
        )
    sh.formula(
        total_row,
        act_c,
        f"=SUM({al}{first}:{al}{last})",
        0,
        fmt=ints,
        font=x._FONT_BODY_BOLD,
        fill=x._FILL_OFF_WHITE,
    )
    sh.put(total_row, stat_c, None, fill=x._FILL_OFF_WHITE)
    dc = stat_c + 1
    dl = _col(dc)
    for i, w in enumerate(weeks):
        sh.inp(first + i, dc, w["days"], ints)
    sh.formula(
        total_row,
        dc,
        f"=SUM({dl}{first}:{dl}{last})",
        sum(w["days"] for w in weeks),
        fmt=ints,
        font=x._FONT_BODY_BOLD,
        fill=x._FILL_OFF_WHITE,
    )
    sh.widths({dc: 12})
    sh.status_rules(f"{_col(stat_c)}{first}:{_col(stat_c)}{last}")
    ws._nova_freeze_row = "C3"

    # campaign weeks value (needs table rows)
    sh.formula(
        weeks_row,
        3,
        f"=SUM({dl}{first}:{dl}{last})/7",
        eff,
        fmt="0.0",
        font=x._FONT_BODY_BOLD,
    )
    sh.merged(
        weeks_row,
        4,
        8,
        "Active days in the weekly table divided by 7: a part-week counts as a part. Every per-week average uses this.",
    )
    refs["weeks"] = (3, weeks_row)

    # ladder rows (now that totals exist)
    steps = (
        ("Minimum test", minf, "E"),
        ("Recommended weekly", None, "D"),
        ("Surge", surf, "F"),
    )
    for i, (label, fref, col_letter) in enumerate(steps):
        r = ladder_first + i
        factor = (DEFAULT_MIN_TEST_FACTOR, 1.0, DEFAULT_SURGE_FACTOR)[i]
        camp = budget * factor
        sh.put(r, 2, label, font=x._FONT_BODY_BOLD)
        if fref:
            sh.formula(r, 3, f"={fref}", factor, fmt=pct)
        else:
            sh.put(r, 3, 1.0, fmt=pct, align=x._ALIGN_CENTER)
        sh.formula(
            r,
            4,
            f"={col_letter}{total_row}/{_a(*refs['weeks'])}",
            camp / eff,
            fmt=cur_fmt,
        )
        sh.formula(r, 5, f"={col_letter}{total_row}", camp, fmt=cur_fmt)
        if cpa:
            clo, chi = _a(*refs["cpa_low"]), _a(*refs["cpa_high"])
            sh.formula(r, 6, f"=D{r}/{chi}", camp / eff / round(cpa[1], 2), fmt=ints)
            sh.formula(r, 7, f"=D{r}/{clo}", camp / eff / round(cpa[0], 2), fmt=ints)
            sh.formula(r, 8, f"=E{r}/{chi}", camp / round(cpa[1], 2), fmt=ints)
            sh.formula(r, 9, f"=E{r}/{clo}", camp / round(cpa[0], 2), fmt=ints)

    # legend rows
    bands = (
        (
            "GREEN",
            f"={green}",
            DEFAULT_GREEN,
            None,
            9.99,
            "On pace: keep spend, keep the plan.",
        ),
        (
            "AMBER",
            f"={amber}",
            DEFAULT_AMBER,
            f"={green}",
            DEFAULT_GREEN,
            "Behind pace: hold budget, fix the cause, review again.",
        ),
        (
            "RED",
            "=0",
            0.0,
            f"={amber}",
            DEFAULT_AMBER,
            "Off pace: pause or reallocate per the rules below.",
        ),
    )
    for i, (word, frm, frm_v, to, to_v, action) in enumerate(bands):
        r = legend_first + i
        cell = sh.put(r, 2, word, font=x._FONT_BODY_BOLD, align=x._ALIGN_CENTER)
        from openpyxl.styles import PatternFill

        cell.fill = PatternFill(
            start_color=_STATUS_COLORS[word],
            end_color=_STATUS_COLORS[word],
            fill_type="solid",
        )
        sh.formula(r, 3, frm, frm_v, fmt=pct)
        if to:
            sh.formula(r, 4, to, to_v, fmt=pct)
        else:
            sh.put(r, 4, "no upper limit", align=x._ALIGN_CENTER)
        sh.merged(r, 5, 8, action)

    # ---- site x week ----
    sites, shares = model["sites"], model["shares"]
    if len(sites) >= 2:
        row = total_row + 2
        last_site_col = 2 + len(sites) + 1
        sh.widths(
            {
                c: 16
                for c in range(9, last_site_col + 1)
                if not ws.column_dimensions[_col(c)].width
                or ws.column_dimensions[_col(c)].width < 16
            }
        )
        row = sh.sub(row, "Site x week split", max(8, last_site_col))
        row = sh.note(
            row,
            f"Each week's recommended spend is split by site share ({model['share_basis']}). "
            "Shares are editable; they must add to 100%.",
            max(8, last_site_col),
        )
        row = sh.header(row, ["Week"] + sites + ["Check"], 2)
        share_row = row
        sh.put(row, 2, "Share of each week's spend", font=x._FONT_BODY_BOLD)
        for j, s in enumerate(shares):
            sh.inp(row, 3 + j, round(s, 4), pct)
        sh.formula(
            row,
            3 + len(sites),
            f'=IF(ABS(SUM(C{row}:{_col(2 + len(sites))}{row})-1)<0.0005,"OK","SHARES MUST ADD TO 100%")',
            (
                "OK"
                if abs(sum(round(s, 4) for s in shares) - 1) < 0.0005
                else "SHARES MUST ADD TO 100%"
            ),
        )
        row += 1
        goals = site_goals(data)
        if goals:
            sh.put(row, 2, "Site hiring goal (hires)", font=x._FONT_BODY_BOLD)
            for j, s_name in enumerate(sites):
                g = demand_planning.goal_for_site(s_name, goals)
                sh.put(row, 3 + j, g if g > 0 else "no goal", fmt="#,##0")
            row += 1
        grid_first = row
        site_tot = [0.0] * len(sites)
        for i, w in enumerate(weeks):
            r = grid_first + i
            alt = x._FILL_BLUE_PALE if i % 2 else x._FILL_WHITE
            sh.put(
                r,
                2,
                f"W{w['n']:02d}  {_date_label(w['start'])}"
                + (" (partial)" if w["partial"] else ""),
                fill=alt,
            )
            rec = budget * w["weight"] / wsum
            for j, s in enumerate(shares):
                v = rec * round(s, 4)
                site_tot[j] += v
                sh.formula(
                    r,
                    3 + j,
                    f"=$D{first + i}*{_col(3 + j)}${share_row}",
                    v,
                    fmt=cur_fmt,
                    fill=alt,
                )
        grid_last = grid_first + n - 1
        r = grid_last + 1
        sh.put(r, 2, "Total", font=x._FONT_BODY_BOLD, fill=x._FILL_OFF_WHITE)
        for j in range(len(sites)):
            c = _col(3 + j)
            sh.formula(
                r,
                3 + j,
                f"=SUM({c}{grid_first}:{c}{grid_last})",
                site_tot[j],
                fmt=cur_fmt,
                font=x._FONT_BODY_BOLD,
                fill=x._FILL_OFF_WHITE,
            )
        diff = sum(site_tot) - tot["rec"]
        sh.formula(
            r,
            3 + len(sites),
            f'=IF(ABS(SUM(C{r}:{_col(2 + len(sites))}{r})-D{total_row})<1,"OK","DOES NOT MATCH PLAN")',
            "OK" if abs(diff) < 1 else "DOES NOT MATCH PLAN",
            font=x._FONT_BODY_BOLD,
            fill=x._FILL_OFF_WHITE,
        )
        row = r + 2
    else:
        row = total_row + 2
        row = sh.note(
            row,
            "Single-site plan: no site split. Add locations in the wizard to get a site x week matrix.",
        )
    sh.note(
        row,
        (
            "Apply starts are estimates from the plan's cost per application, not a promise; "
            if cpa
            else "No apply-start estimate is made without a cost per application; "
        )
        + "the client's system of record decides what counts (see Tracking & Measurement).",
        max(8, ncol),
    )
    return {
        "refs": refs,
        "first": first,
        "last": last,
        "total_row": total_row,
        "sh": sh,
    }


# -- Publisher Tiers & Gates ---------------------------------------------------
def _build_tiers(
    ws: Any, data: dict, model: Dict[str, Any], x: Any, pacing: Dict[str, Any]
) -> Dict[str, Any]:
    sh = _Sheet(ws, x)
    ws.title = SHEET_TIERS
    sh.widths(
        {
            1: 3,
            2: 30,
            3: 16,
            4: 16,
            5: 13,
            6: 15,
            7: 11,
            8: 16,
            9: 16,
            10: 14,
            11: 28,
            12: 16,
            13: 16,
            14: 22,
        }
    )
    cur_fmt, pct, ints = x._usd0_fmt(), x.FMT_PCT1, x.FMT_INT
    prefs = pacing["refs"]
    wk1_ref = _xref(SHEET_PACING, 4, pacing["first"])
    wk1_value = (
        model["total_budget"]
        * model["weeks"][0]["weight"]
        / (sum(w["weight"] for w in model["weeks"]) or 1.0)
    )
    weeks_ref = _xref(SHEET_PACING, *prefs["weeks"])
    minf_ref = _xref(SHEET_PACING, *prefs["min_factor"])
    amber_ref = _xref(SHEET_PACING, *prefs["amber"])
    green_ref = _xref(SHEET_PACING, *prefs["green"])
    n_weeks = model["effective_weeks"] or 1.0

    row = _title_block(
        sh,
        "Publisher Tiers & Gates",
        "Which publishers launch first, which wait for a gate, and what moves budget between them. "
        "Built from the plan's funded publishers, after client exclusions and availability.",
        12,
    )
    if model["excluded"]:
        try:
            import publisher_filter as _pf

            _line = _pf.disclosure_line(data)
        except ImportError:  # pragma: no cover
            _line = ""
        if _line:
            row = sh.note(row - 1, _line, 12) + 1

    row = sh.sub(row, "Tier rules (editable)", 12)
    row = sh.header(row, ["Rule", "Value", "Basis"], 2)
    ws.merge_cells(start_row=row - 1, start_column=4, end_row=row - 1, end_column=8)
    sh.put(row, 2, "Tier 1 up to cumulative budget share", font=x._FONT_BODY_BOLD)
    sh.inp(row, 3, DEFAULT_TIER1_CUM, pct)
    sh.merged(
        row, 4, 8, f"{DEFAULT_PLAN_LABEL}. Largest publishers, launched in week 1."
    )
    t1_row = row
    row += 1
    sh.put(row, 2, "Tier 2 up to cumulative budget share", font=x._FONT_BODY_BOLD)
    sh.inp(row, 3, DEFAULT_TIER2_CUM, pct)
    sh.merged(
        row,
        4,
        8,
        f"{DEFAULT_PLAN_LABEL}. Launched once Gate 1 passes. The rest are Tier 3 (test).",
    )
    t2_row = row
    row += 2

    row = sh.sub(row, "Launch plan by publisher", 12)
    row = sh.header(
        row,
        [
            "Publisher",
            "Category",
            "Planned budget",
            "Share",
            "Cumulative share before",
            "Tier",
            "Planned weekly spend",
            "Minimum weekly spend",
            "Meets minimum?",
            "Launch rule",
            "Plan applications",
            "Week 1 spend",
            "Runs in",
        ],
        2,
    )
    pubs = tiered_publishers(model)
    tier1_budget = sum(p["budget"] for p in pubs if p["tier"] == "Tier 1")
    first = row
    last = row + max(len(pubs), 1) - 1
    total = sum(p["budget"] for p in pubs) or 1.0
    t1, t2 = _a(3, t1_row), _a(3, t2_row)
    for i, p in enumerate(pubs):
        r = first + i
        alt = x._FILL_BLUE_PALE if i % 2 else x._FILL_WHITE
        sh.put(r, 2, p["name"], font=x._FONT_BODY_BOLD, fill=alt)
        sh.put(r, 3, (p["category"] or "").replace("_", " ").title(), fill=alt)
        sh.put(r, 4, p["budget"], fmt=cur_fmt, fill=alt, align=x._ALIGN_CENTER)
        sh.formula(
            r, 5, f"=D{r}/SUM($D${first}:$D${last})", p["share"], fmt=pct, fill=alt
        )
        before = p["cum_before"]
        if i == 0:
            sh.formula(r, 6, "=0", 0.0, fmt=pct, fill=alt)
        else:
            sh.formula(r, 6, f"=F{r - 1}+E{r - 1}", before, fmt=pct, fill=alt)
        sh.formula(
            r,
            7,
            f'=IF(F{r}<{t1},"Tier 1",IF(F{r}<{t2},"Tier 2","Tier 3"))',
            p["tier"],
            fill=alt,
        )
        weekly = p["budget"] / n_weeks
        sh.formula(r, 8, f"=D{r}/{weeks_ref}", weekly, fmt=cur_fmt, fill=alt)
        minimum = _xl_round(weekly * DEFAULT_MIN_TEST_FACTOR)
        sh.formula(
            r, 9, f"=ROUND(H{r}*{minf_ref},0)", minimum, fmt=cur_fmt, as_input=True
        )
        sh.formula(
            r,
            10,
            f'=IF(H{r}>=I{r},"OK","BELOW MINIMUM")',
            "OK" if weekly >= minimum else "BELOW MINIMUM",
            fill=alt,
        )
        launch = {
            "Tier 1": "Launch in week 1",
            "Tier 2": "Launch after Gate 1 passes",
            "Tier 3": "Test after Gate 2 passes",
        }[p["tier"]]
        sh.formula(
            r,
            11,
            f'=IF(G{r}="Tier 1","Launch in week 1",IF(G{r}="Tier 2","Launch after Gate 1 passes","Test after Gate 2 passes"))',
            launch,
            fill=alt,
        )
        sh.put(r, 12, p["applications"], fmt=ints, fill=alt, align=x._ALIGN_CENTER)
        sh.formula(
            r,
            13,
            f'=IF(G{r}="Tier 1",{wk1_ref}*D{r}/SUMIF($G${first}:$G${last},"Tier 1",$D${first}:$D${last}),0)',
            (
                (wk1_value * p["budget"] / tier1_budget)
                if p["tier"] == "Tier 1" and tier1_budget
                else 0.0
            ),
            fmt=cur_fmt,
            fill=alt,
        )
        runs_in = (
            "All plan markets"
            if p["all_markets"]
            else ", ".join(p["markets"]) + " only"
        )
        sh.put(r, 14, runs_in, fill=alt)
    if not pubs:
        sh.merged(
            first,
            2,
            12,
            "The plan funds no publisher: nothing to tier. Check budget and exclusions.",
        )
        last = first
    rr = last + 1
    sh.put(rr, 2, "Total", font=x._FONT_BODY_BOLD, fill=x._FILL_OFF_WHITE)
    sh.put(rr, 3, None, fill=x._FILL_OFF_WHITE)
    sh.formula(
        rr,
        4,
        f"=SUM(D{first}:D{last})",
        sum(p["budget"] for p in pubs),
        fmt=cur_fmt,
        font=x._FONT_BODY_BOLD,
        fill=x._FILL_OFF_WHITE,
    )
    sh.formula(
        rr,
        5,
        f"=SUM(E{first}:E{last})",
        1.0 if pubs else 0.0,
        fmt=pct,
        font=x._FONT_BODY_BOLD,
        fill=x._FILL_OFF_WHITE,
    )
    row = rr + 1
    row = sh.note(
        row,
        f"Minimum weekly spend defaults to the minimum-test factor on {SHEET_PACING} times the publisher's planned weekly spend; "
        "overwrite it with the publisher's own minimum.",
        14,
    )
    row = sh.note(
        row,
        f"{SHEET_PACING} governs total spend. 'Planned weekly spend' is each publisher's average once every tier is live. "
        "Tier 2 and Tier 3 launch after their gates, so in week 1 the Tier 1 publishers carry that week's recommended spend "
        "('Week 1 spend'), which is above their average until the gates pass.",
        14,
    )
    if model["dropped_count"]:
        row = sh.note(
            row,
            f"{model['dropped_count']} publisher(s) in the plan's budget do not run in this plan's market(s) and are left out "
            f"of this launch plan; their {x._fmt_currency(model['dropped_budget'])} needs reallocating to publishers that do. "
            "A publisher marked 'only' in 'Runs in' runs in just those markets.",
            14,
        )
    elif any(not p["all_markets"] for p in pubs):
        row = sh.note(
            row,
            "A publisher marked 'only' in 'Runs in' runs in just those markets; its budget serves only the sites there.",
            14,
        )
    row += 1

    # switch triggers
    row = sh.sub(row, "Switch triggers (defaults — set to the client's SLA)", 12)
    row = sh.header(
        row,
        [
            "Trigger",
            "Threshold (% of apply-start goal)",
            "Sustained for (days)",
            "Action",
        ],
        2,
    )
    ws.merge_cells(start_row=row - 1, start_column=5, end_row=row - 1, end_column=9)
    triggers = (
        (
            "Move budget away from a Tier 1 publisher that stays below the threshold",
            f"={amber_ref}",
            DEFAULT_AMBER,
            DEFAULT_REALLOCATE_DAYS,
            "Move its next-week budget to Tier 1 publishers that are GREEN.",
        ),
        (
            "Promote a Tier 2 publisher that stays at or above the threshold",
            None,
            DEFAULT_PROMOTE_PCT,
            DEFAULT_PROMOTE_DAYS,
            "Fund it at Tier 1 level from the next weekly budget reset.",
        ),
        (
            "Pause any publisher that stays below the threshold",
            f"={amber_ref}",
            DEFAULT_AMBER,
            DEFAULT_PAUSE_DAYS,
            "Pause, reallocate, and confirm tracking before relaunching.",
        ),
    )
    for label, f, v, days, action in triggers:
        sh.put(row, 2, label, font=x._FONT_BODY_BOLD)
        if f:
            sh.formula(row, 3, f, v, fmt=pct, as_input=True)
        else:
            sh.inp(row, 3, v, pct)
        sh.inp(row, 4, days, ints)
        sh.merged(row, 5, 9, action)
        ws.row_dimensions[row].height = 30
        row += 1
    row += 1

    # performance gates
    row = sh.sub(
        row, "Performance gates by week (defaults — set to the client's SLA)", 12
    )
    row = sh.header(
        row,
        ["Gate", "At end of week", "Pass at (% of goal)", "Measure", "If it fails"],
        2,
    )
    ws.merge_cells(start_row=row - 1, start_column=6, end_row=row - 1, end_column=9)
    ws.merge_cells(start_row=row - 1, start_column=10, end_row=row - 1, end_column=12)
    gates = (
        (
            "Gate 0: tracking validated",
            0,
            "=1",
            1.0,
            "Every link records a test apply in the system of record.",
            "Do not launch the publisher.",
        ),
        (
            "Gate 1: Tier 1 live",
            1,
            f"={amber_ref}",
            DEFAULT_AMBER,
            "Tier 1 apply starts vs weekly goal.",
            "Fix tracking and ad quality; do not launch Tier 2.",
        ),
        (
            "Gate 2: Tier 2 live",
            2,
            f"={amber_ref}",
            DEFAULT_AMBER,
            "Tier 1 and Tier 2 apply starts vs weekly goal.",
            "Hold Tier 3; reallocate per the switch triggers.",
        ),
        (
            "Gate 3: steady state",
            4,
            f"={green_ref}",
            DEFAULT_GREEN,
            "Total apply starts vs weekly goal, every week.",
            "Reallocate or pause per the switch triggers.",
        ),
    )
    for label, wk, f, v, measure, fail in gates:
        sh.put(row, 2, label, font=x._FONT_BODY_BOLD)
        sh.inp(row, 3, wk, ints)
        sh.formula(row, 4, f, v, fmt=pct, as_input=True)
        sh.merged(row, 5, 9, measure)
        sh.merged(row, 10, 12, fail)
        ws.row_dimensions[row].height = 30
        row += 1
    return {"first": first, "last": last, "pubs": pubs}


# -- Tracking & Measurement ---------------------------------------------------
_SOCIAL_RE = re.compile(
    r"meta|facebook|instagram|tiktok|linkedin|snap|pinterest|reddit|twitter|youtube|social",
    re.I,
)


def _medium_for(name: str, category: str = "") -> str:
    """utm_medium for a publisher: judged by its name first (the budget
    engine files social platforms under "job_board"), then its category."""
    if re.search(r"programmatic", name, re.I) or category == "programmatic":
        return "programmatic"
    if _SOCIAL_RE.search(name) or category == "social":
        return "paid-social"
    if re.search(r"google|bing|search ads|sem\b", name, re.I):
        return "paid-search"
    return "jobboard"


def _site_codes(sites: List[str]) -> List[str]:
    codes: List[str] = []
    for s in sites:
        base = (re.sub(r"[^a-z]", "", s.split(",")[0].lower())[:3] or "site").upper()
        code, k = base, 2
        while code in codes:
            code, k = f"{base}{k}", k + 1
        codes.append(code)
    return codes


def tracking_spec(model: Dict[str, Any]) -> Dict[str, Any]:
    """The UTM / campaign-ID convention generated for this plan."""
    pubs = tiered_publishers(model)
    slug = model["plan_slug"]
    sites = model["sites"] or ["Plan"]
    codes = _site_codes(sites)
    first_pub = pubs[0] if pubs else None
    src = _slug(first_pub["name"]) if first_pub else "publisher"
    medium = (
        _medium_for(first_pub["name"], first_pub["category"])
        if first_pub
        else "jobboard"
    )
    role = _slug(model["roles"][0]) if model["roles"] else "role"
    links: List[Dict[str, str]] = []
    site_country = model.get("site_country") or {}
    for site, code in zip(sites, codes):
        for p in pubs:
            country = site_country.get(site)
            if p["markets"] and country and country not in p["markets"]:
                continue
            psrc = _slug(p["name"])
            pmed = _medium_for(p["name"], p["category"])
            camp = f"{slug}_{code.lower()}_w01"
            links.append(
                {
                    "site": site,
                    "publisher": p["name"],
                    "params": f"utm_source={psrc}&utm_medium={pmed}&utm_campaign={camp}",
                    "campaign_id": f"{slug}-{code.lower()}-{psrc}-w01",
                }
            )
    return {
        "slug": slug,
        "codes": codes,
        "example_source": src,
        "example_medium": medium,
        "example_campaign": f"{slug}_{codes[0].lower()}_w01",
        "example_content": role,
        "example_id": f"{slug}-{codes[0].lower()}-{src}-w01",
        "links": links,
    }


def _build_tracking(
    ws: Any, data: dict, model: Dict[str, Any], x: Any
) -> Dict[str, Any]:
    sh = _Sheet(ws, x)
    ws.title = SHEET_TRACKING
    sh.widths({1: 3, 2: 30, 3: 36, 4: 56, 5: 20, 6: 18})
    ints, pct = x.FMT_INT, x.FMT_PCT1
    cfg = model["settings"]
    spec = tracking_spec(model)

    row = _title_block(
        sh,
        "Tracking & Measurement",
        "How every apply is attributed: one naming convention on every link, one system of record, "
        "and a rule for when two systems disagree.",
        6,
    )
    row = sh.sub(row, "System of record", 6)
    sh.put(row, 2, "System of record for applies", font=x._FONT_BODY_BOLD)
    sor_value = cfg["system_of_record"] or DEFAULT_SYSTEM_OF_RECORD
    sh.inp(row, 3, sor_value)
    sh.merged(
        row,
        4,
        6,
        "Where an apply officially counts. Joveo and publisher reports are inputs; this system settles any dispute. "
        + (
            "Provided in the wizard."
            if cfg["system_of_record"]
            else f"{DEFAULT_LABEL}: confirm with the client."
        ),
    )
    sor_ref = (3, row)
    row += 2

    row = sh.sub(row, "Naming convention", 6)
    row = sh.header(
        row, ["Field", "Convention", "Example for this plan", "Required", ""], 2
    )
    ws.cell(row=row - 1, column=6).value = None
    convention = (
        (
            "utm_source",
            "Publisher slug: lower case, words joined by hyphens",
            spec["example_source"],
            "Every link",
        ),
        (
            "utm_medium",
            "Channel type: jobboard, programmatic, paid-social, paid-search",
            spec["example_medium"],
            "Every link",
        ),
        (
            "utm_campaign",
            "{plan}_{site code}_{campaign week}",
            spec["example_campaign"],
            "Every link",
        ),
        (
            "utm_content",
            "Role or job-family slug (optional, for role-level reads)",
            spec["example_content"],
            "Optional",
        ),
        (
            "Campaign ID",
            "{plan}-{site code}-{publisher}-{campaign week}; the same ID in every report",
            spec["example_id"],
            "Every report",
        ),
        (
            "Site code",
            "First three letters of the city, upper case; a clash gets a number",
            ", ".join(spec["codes"][:6]),
            "Every link",
        ),
    )
    for i, (f, c, e, r) in enumerate(convention):
        alt = x._FILL_BLUE_PALE if i % 2 else x._FILL_WHITE
        sh.put(row, 2, f, font=x._FONT_BODY_BOLD, fill=alt)
        sh.put(row, 3, c, fill=alt)
        sh.put(row, 4, e, fill=alt)
        sh.put(row, 5, r, fill=alt)
        row += 1
    row = sh.note(
        row,
        "Rule: a link missing any required field is not launched. Week codes count from campaign week 1 (see Weekly Pacing).",
        6,
    )
    row += 1

    row = sh.sub(row, "Link parameters, campaign week 1", 6)
    row = sh.note(
        row,
        "Append these to each publisher's apply URL (replace w01 with the campaign week). Capped at 60 rows.",
        6,
    )
    row = sh.header(row, ["Site", "Publisher", "Tracking parameters", "Campaign ID"], 2)
    for i, link in enumerate(spec["links"][:60]):
        alt = x._FILL_BLUE_PALE if i % 2 else x._FILL_WHITE
        sh.put(row, 2, link["site"], fill=alt)
        sh.put(row, 3, link["publisher"], fill=alt)
        sh.put(row, 4, link["params"], fill=alt)
        sh.put(row, 5, link["campaign_id"], fill=alt)
        row += 1
    if len(spec["links"]) > 60:
        row = sh.note(
            row,
            f"+{len(spec['links']) - 60} more site and publisher combinations follow the same pattern.",
            6,
        )
    row += 1

    row = sh.sub(row, "Reconciliation rule", 6)
    sh.put(row, 2, "Tolerance between systems", font=x._FONT_BODY_BOLD)
    sh.inp(row, 3, DEFAULT_RECON_TOLERANCE, pct)
    sh.merged(
        row,
        4,
        6,
        f"{DEFAULT_LABEL}. Variance above this on two consecutive days triggers the steps below.",
    )
    tol_ref = (3, row)
    row += 1
    for line in (
        "1. The system of record is correct by definition; publisher and Joveo numbers are compared to it, never the reverse.",
        "2. When two systems differ by more than the tolerance for two consecutive days, freeze budget moves for that publisher.",
        "3. Compare by campaign ID: check the link parameters, the redirect and the apply flow, in that order.",
        "4. Log the cause and the fix, then lift the freeze once a test apply reconciles.",
    ):
        row = sh.note(row, line, 6)
    row += 1
    row = sh.sub(row, "Reconciliation worksheet (enter daily or weekly counts)", 6)
    row = sh.header(
        row,
        [
            "Publisher",
            "Applies per Joveo",
            "Applies per system of record",
            "Variance",
            "Within tolerance?",
        ],
        2,
    )
    rec_first = row
    pubs = tiered_publishers(model)[:25]
    tol = _a(*tol_ref)
    for p in pubs:
        sh.put(row, 2, p["name"], font=x._FONT_BODY_BOLD)
        sh.inp(row, 3, None, ints)
        sh.inp(row, 4, None, ints)
        sh.formula(
            row,
            5,
            f'=IF(OR(C{row}="",D{row}="",D{row}=0),"",C{row}/D{row}-1)',
            "",
            fmt=pct,
        )
        sh.formula(
            row, 6, f'=IF(E{row}="","",IF(ABS(E{row})<={tol},"OK","RECONCILE"))', ""
        )
        row += 1
    if pubs:
        from openpyxl.formatting.rule import CellIsRule
        from openpyxl.styles import PatternFill

        ws.conditional_formatting.add(
            f"F{rec_first}:F{row - 1}",
            CellIsRule(
                operator="equal",
                formula=['"RECONCILE"'],
                fill=PatternFill(
                    start_color="FECACA", end_color="FECACA", fill_type="solid"
                ),
            ),
        )
    row += 1

    row = sh.sub(row, "Launch checklist", 6)
    row = sh.header(row, ["Step", "", "Detail", "Owner (enter)", "Done?"], 2)
    ws.merge_cells(start_row=row - 1, start_column=2, end_row=row - 1, end_column=3)
    lock = cfg["lock_day"].capitalize() if cfg["lock_day"] else "not set — confirm"
    sla = (
        f"{cfg['setup_sla_days']} business days"
        if cfg["setup_sla_days"] is not None
        else "not set — confirm"
    )
    steps = (
        (
            "Validate tracking before launch",
            "Open every link, complete a test apply, and see it in the system of record under the right campaign ID.",
        ),
        (
            "Re-check at 24 hours",
            "Compare publisher-reported clicks and applies to the system of record; reconcile any gap above the tolerance.",
        ),
        (
            "Fix window",
            f"A tracking fault is fixed within {DEFAULT_FIX_WINDOW_HOURS} hours ({DEFAULT_LABEL}); budget on that publisher is frozen until then.",
        ),
        (
            "Request lock day",
            f"{lock}. Changes requested after this day start the following fiscal week.",
        ),
        ("Setup SLA", f"{sla}, counted from an approved request to a live publisher."),
        (
            "Re-validate after any change",
            "Any change to the apply flow, ATS or redirect repeats the pre-launch test.",
        ),
    )
    chk_first = row
    for i, (step, detail) in enumerate(steps):
        sh.merged(row, 2, 3, step, font=x._FONT_BODY_BOLD)
        sh.put(row, 4, detail)
        sh.inp(row, 5, None)
        sh.inp(row, 6, None)
        row += 1
    sh.dropdown(f"F{chk_first}:F{row - 1}", ["Yes", "No", "N/A"])
    return {"sor_ref": sor_ref, "sor_value": sor_value}


# -- Feasibility Check --------------------------------------------------------
def _build_feasibility(
    ws: Any, data: dict, model: Dict[str, Any], x: Any, tracking: Dict[str, Any]
) -> None:
    sh = _Sheet(ws, x)
    ws.title = SHEET_FEASIBILITY
    sh.widths({1: 3, 2: 38, 3: 14, 4: 60, 5: 22, 6: 48})
    ints = x.FMT_INT
    cfg = model["settings"]

    row = _title_block(
        sh,
        "Feasibility Check",
        "A pre-plan gate: can this plan run as written? PASS, CHECK and FAIL come from the plan's own data; "
        "CONFIRM marks what only the client can answer.",
        6,
    )
    sum_row = row
    row += 7  # summary block (filled after the rows exist)
    row = sh.sub(row, "Inputs (editable)", 6)
    row = sh.header(row, ["Input", "Value", "Basis"], 2)
    ws.merge_cells(start_row=row - 1, start_column=4, end_row=row - 1, end_column=6)

    def inp_row(
        label: str, value: Any, basis: str, fmt: Optional[str] = None
    ) -> Tuple[int, int]:
        nonlocal row
        sh.put(row, 2, label, font=x._FONT_BODY_BOLD)
        sh.inp(row, 3, value, fmt)
        sh.merged(row, 4, 6, basis)
        ref = (3, row)
        row += 1
        return ref

    min_ref = inp_row(
        "Minimum publishers per market",
        DEFAULT_MIN_MARKET_BOARDS,
        f"{DEFAULT_PLAN_LABEL}. Fewer than this and a market is flagged CHECK.",
        ints,
    )
    goal_ref = inp_row(
        "Hiring goal (hires)",
        model["hire_goal"] or None,
        "From the plan's openings or hire volume; enter it if blank.",
        ints,
    )
    lo_ref = inp_row(
        "Projected hires, low",
        round(model["hires_low"]) or None,
        "From the plan's budget projection.",
        ints,
    )
    hi_ref = inp_row(
        "Projected hires, high",
        round(model["hires_high"]) or None,
        "From the plan's budget projection.",
        ints,
    )
    lock_ref = inp_row(
        "Request lock day",
        cfg["lock_day"].capitalize() or None,
        "From the wizard; the last day a change can be requested for the next fiscal week.",
    )
    sla_ref = inp_row(
        "Setup SLA (business days)",
        cfg["setup_sla_days"],
        "From the wizard; days from an approved request to a live publisher.",
        ints,
    )
    row += 1

    row = sh.sub(row, "Checks", 6)
    row = sh.header(
        row,
        [
            "Check",
            "Status",
            "Evidence from the plan",
            "Confirmation (enter)",
            "Next step",
        ],
        2,
    )
    first = row
    minc, goal, lo, hi = (_a(*min_ref), _a(*goal_ref), _a(*lo_ref), _a(*hi_ref))
    lock_c, sla_c = _a(*lock_ref), _a(*sla_ref)
    sor_cell = _xref(SHEET_TRACKING, *tracking["sor_ref"])

    markets = model["markets"]
    for m in markets:
        n = m["boards"]
        sh.put(row, 2, f"Supply in market: {m['country']}", font=x._FONT_BODY_BOLD)
        sh.formula(
            row,
            3,
            f'=IF(E{row}>={minc},"PASS","CHECK")',
            "PASS" if n >= DEFAULT_MIN_MARKET_BOARDS else "CHECK",
        )
        names = ", ".join(m["board_names"][:5])
        sh.put(
            row,
            4,
            f"{n} publishers available across {len(m['sites'])} site(s): {', '.join(m['sites'][:4])}"
            + (f". Includes {names}." if names else "."),
        )
        sh.inp(row, 5, n, ints)
        sh.put(row, 6, "Confirm each site can take applies from the listed publishers.")
        row += 1
    if not markets:
        sh.put(row, 2, "Supply in market", font=x._FONT_BODY_BOLD)
        sh.put(row, 3, "CONFIRM", align=x._ALIGN_CENTER)
        sh.put(row, 4, "The plan names no location.")
        sh.inp(row, 5, None)
        sh.put(row, 6, "Add locations in the wizard.")
        row += 1
    if model["excluded"]:
        remaining = len(model["channels"])
        sh.put(row, 2, "Publishers excluded at client request", font=x._FONT_BODY_BOLD)
        sh.formula(
            row, 3, f'=IF(E{row}>0,"PASS","FAIL")', "PASS" if remaining else "FAIL"
        )
        sh.put(
            row,
            4,
            f"{len(model['excluded'])} publisher(s) excluded at client request; the plan still funds {remaining} publishers.",
        )
        sh.inp(row, 5, remaining, ints)
        sh.put(row, 6, "FAIL means exclusions leave nothing to fund.")
        row += 1
    flags = model["compliance_flags"]
    sh.put(row, 2, "Compliance flags", font=x._FONT_BODY_BOLD)
    sh.formula(
        row,
        3,
        f'=IF(E{row}="Reviewed","PASS","{"CHECK" if flags else "CONFIRM"}")',
        "CHECK" if flags else "CONFIRM",
    )
    sh.put(
        row,
        4,
        (
            " ".join(flags)
            if flags
            else "No compliance flags were computed for this plan. Confirm local pay-transparency, consent and language rules."
        ),
    )
    sh.inp(row, 5, "Not reviewed")
    sh.dropdown(f"E{row}", ["Reviewed", "Not reviewed"])
    sh.put(row, 6, "Have the client's compliance owner review, then set to Reviewed.")
    row += 1
    sh.put(row, 2, "ATS redirect and tracking support", font=x._FONT_BODY_BOLD)
    sh.formula(
        row, 3, f'=IF(E{row}="Yes","PASS",IF(E{row}="No","FAIL","CONFIRM"))', "CONFIRM"
    )
    sh.put(
        row,
        4,
        "Does the client's apply flow accept a redirect from the publisher and pass UTM parameters through to the system of record?",
    )
    sh.inp(row, 5, "Unknown")
    sh.dropdown(f"E{row}", ["Yes", "No", "Unknown"])
    sh.put(
        row, 6, "Test one link end to end before launch (see Tracking & Measurement)."
    )
    row += 1
    goal_v, lo_v, hi_v = (
        model["hire_goal"],
        round(model["hires_low"]),
        round(model["hires_high"]),
    )
    if goal_v <= 0 or hi_v <= 0:
        scale = "CONFIRM"
    else:
        scale = "PASS" if lo_v >= goal_v else "CHECK" if hi_v >= goal_v else "FAIL"
    sh.put(row, 2, "Scale vs target", font=x._FONT_BODY_BOLD)
    sh.formula(
        row,
        3,
        f'=IF(OR({goal}<=0,{hi}<=0),"CONFIRM",IF({lo}>={goal},"PASS",IF({hi}>={goal},"CHECK","FAIL")))',
        scale,
    )
    sh.put(
        row,
        4,
        "The plan's projected hires range against the hiring goal (inputs above). PASS: the low end meets the goal. CHECK: only the high end does. FAIL: neither.",
    )
    sh.put(row, 5, None)
    sh.put(
        row, 6, "FAIL or CHECK: raise the budget, extend the window or lower the goal."
    )
    row += 1
    both = bool(cfg["lock_day"]) and cfg["setup_sla_days"] is not None
    sh.put(row, 2, "Lead times (request lock day, setup SLA)", font=x._FONT_BODY_BOLD)
    sh.formula(
        row,
        3,
        f'=IF(AND({lock_c}<>"",{sla_c}<>""),"PASS","CONFIRM")',
        "PASS" if both else "CONFIRM",
    )
    sh.put(row, 4, "Both lead times are needed to know when a change can take effect.")
    sh.put(row, 5, None)
    sh.put(row, 6, "Enter both inputs above.")
    row += 1
    sh.put(row, 2, "System of record named", font=x._FONT_BODY_BOLD)
    named = bool(cfg["system_of_record"])
    sh.formula(
        row,
        3,
        f'=IF(OR({sor_cell}="",{sor_cell}="{DEFAULT_SYSTEM_OF_RECORD}"),"CONFIRM","PASS")',
        "PASS" if named else "CONFIRM",
    )
    sh.put(
        row,
        4,
        "Read from Tracking & Measurement: "
        + (cfg["system_of_record"] or "still the placeholder."),
    )
    sh.put(row, 5, None)
    sh.put(row, 6, "Name the system that counts applies.")
    row += 1
    last = row - 1
    sh.verdict_rules(f"C{first}:C{last}")

    # summary block
    rows = feasibility_rows(model)
    counts = {k: 0 for k in ("PASS", "CHECK", "FAIL", "CONFIRM")}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    r0 = sum_row
    r = sh.sub(r0, "Summary", 6)
    for word in ("PASS", "CHECK", "FAIL", "CONFIRM"):
        sh.put(r, 2, word, font=x._FONT_BODY_BOLD)
        sh.formula(
            r, 3, f'=COUNTIF($C${first}:$C${last},"{word}")', counts[word], fmt=ints
        )
        r += 1
    verdict = (
        "NOT READY"
        if counts["FAIL"]
        else (
            "READY AFTER CONFIRMATIONS"
            if counts["CHECK"] or counts["CONFIRM"]
            else "READY"
        )
    )
    # r now = r0 + 5; verdict goes on the sub-header row's right side
    sh.put(r, 2, "Gate result", font=x._FONT_BODY_BOLD)
    fail_c, chk_c, conf_c = (f"C{r0 + 1 + 2}", f"C{r0 + 1 + 1}", f"C{r0 + 1 + 3}")
    sh.formula(
        r,
        3,
        f'=IF({fail_c}>0,"NOT READY",IF({chk_c}+{conf_c}>0,"READY AFTER CONFIRMATIONS","READY"))',
        verdict,
        font=x._FONT_BODY_BOLD,
    )
    ws.merge_cells(start_row=r, start_column=3, end_row=r, end_column=4)
    ws.cell(row=r, column=4).border = x._BORDER_THIN


# -- Job Ad Quality ------------------------------------------------------------
def jobad_rows(model: Dict[str, Any]) -> List[Dict[str, str]]:
    """The job-ad checklist with the flags the plan can detect."""
    roles = model["roles"]
    countries = [c for c in model["countries"] if c]
    non_us = [c for c in countries if c != "United States"]
    sites = model["site_count"]
    rows: List[Dict[str, str]] = []
    rows.append(
        {
            "check": "Pay is shown",
            "good": "Every ad states the pay rate or range, in the local currency.",
            "flag": (
                "A pay figure appears in the brief; carry it into every ad."
                if model["pay_in_brief"]
                else "No pay figure was found in the brief. Add the pay range to every ad."
            ),
        }
    )
    rows.append(
        {
            "check": "Shift and schedule stated",
            "good": "Shift pattern, hours per week and start dates are in the ad, per role.",
            "flag": (
                f"{len(roles)} role(s) in the plan ({', '.join(roles[:4])}): confirm the shift pattern is stated for each."
                if roles
                else "No role named in the plan; state the shift pattern in each ad."
            ),
        }
    )
    rows.append(
        {
            "check": "Language requirements",
            "good": "The ad is written in the market's language and states any language requirement.",
            "flag": (
                f"The plan covers {', '.join(countries)}: confirm each ad's language and any requirement."
                if non_us
                else "US-only plan: state any language requirement (for example bilingual) explicitly."
            ),
        }
    )
    claims = model["brief_claims"]
    flag = (
        f"{sites} sites in the plan: any benefit, bonus, shift or start-date promise must hold at every site, or be site-specific."
        if sites > 1
        else "Single-site plan: promises need to hold at that site."
    )
    if claims:
        flag += (
            " The brief mentions: "
            + ", ".join(claims)
            + "; confirm each site can fulfil them."
        )
    rows.append(
        {
            "check": "No promise a site cannot deliver",
            "good": "Nothing in the ad is promised that a site cannot fulfil.",
            "flag": flag,
        }
    )
    jt = model["jargon_titles"]
    rows.append(
        {
            "check": "Plain-language job title",
            "good": "The title is what a candidate would search for: no internal levels, codes or slashes.",
            "flag": (
                "Check these titles for internal wording: " + "; ".join(jt[:5]) + "."
                if jt
                else "No internal codes or levels detected in the plan's role titles."
            ),
        }
    )
    rows.append(
        {
            "check": "Apply link works and is tracked",
            "good": "The apply link opens on a phone and carries the tracking parameters.",
            "flag": "See Tracking & Measurement for the parameters and the pre-launch test.",
        }
    )
    return rows


def _build_jobad(ws: Any, data: dict, model: Dict[str, Any], x: Any) -> None:
    sh = _Sheet(ws, x)
    ws.title = SHEET_JOBAD
    sh.widths({1: 3, 2: 32, 3: 54, 4: 60, 5: 14})
    ints = x.FMT_INT
    row = _title_block(
        sh,
        "Job Ad Quality",
        "A pre-launch checklist for the ads the publishers will run. The plan flags what it can detect; "
        "a person marks each item Pass, Fix or N/A.",
        5,
    )
    row = sh.header(
        row,
        ["Check", "What good looks like", "Flag from the plan", "Status (select)"],
        2,
    )
    first = row
    for i, r in enumerate(jobad_rows(model)):
        alt = x._FILL_BLUE_PALE if i % 2 else x._FILL_WHITE
        sh.put(row, 2, r["check"], font=x._FONT_BODY_BOLD, fill=alt)
        sh.put(row, 3, r["good"], fill=alt)
        sh.put(row, 4, r["flag"], fill=alt)
        sh.inp(row, 5, None)
        ws.row_dimensions[row].height = 44
        row += 1
    last = row - 1
    sh.dropdown(f"E{first}:E{last}", ["Pass", "Fix", "N/A"])
    row += 1
    sh.put(row, 2, "Items marked Fix", font=x._FONT_BODY_BOLD)
    sh.formula(row, 3, f'=COUNTIF(E{first}:E{last},"Fix")', 0, fmt=ints)
    sh.put(row + 1, 2, "Items still open", font=x._FONT_BODY_BOLD)
    sh.formula(
        row + 1,
        3,
        f'=COUNTA(B{first}:B{last})-COUNTIF(E{first}:E{last},"Pass")-COUNTIF(E{first}:E{last},"N/A")-COUNTIF(E{first}:E{last},"Fix")',
        last - first + 1,
        fmt=ints,
    )


# -- Goal Plan (demand-first) -------------------------------------------------
def goal_plan_of(data: Any) -> Optional[Dict[str, Any]]:
    """The stamped goal plan for an enabled plan, else None."""
    if not enabled(data):
        return None
    gp = (data.get("_budget_allocation") or {}).get("goal_plan")
    return gp if isinstance(gp, dict) and gp.get("ok") else None


def _build_goalplan(ws: Any, data: dict, gp: Dict[str, Any], x: Any) -> None:
    sh = _Sheet(ws, x)
    ws.title = SHEET_GOALPLAN
    sh.widths({1: 3, 2: 38, 3: 18, 4: 18, 5: 18, 6: 18, 7: 16, 8: 16, 9: 48})
    cur_fmt = x._usd0_fmt()
    ints = x.FMT_INT
    pct = x.FMT_PCT1
    unit = gp["unit"]
    last = 9

    row = _title_block(
        sh,
        "Goal Plan",
        f"Plan backward from the goal: what {unit} the goal needs, what the budget "
        "buys, and three ways to decide. Every figure is the plan's own projection "
        "inverted -- no client hire history is used, so these are estimates, not forecasts.",
        last,
    )
    if gp.get("status") == "not_projected":
        row = sh.sub(row, "Goal not priced", last)
        row = sh.note(row, str(gp["text"]["callout"]), last)
        return
    if gp.get("status") not in ("meets", "borderline", "short"):
        row = sh.sub(row, "Goal not priced", last)
        row = sh.note(row, str(gp["text"]["callout"]), last)
        return

    req, proj, short = gp["required"], gp["projected"], gp["shortfall"]
    row = sh.sub(row, "Goal and budget (editable)", last)
    row = sh.header(row, ["Input", "Value", "Basis"], 2)
    ws.merge_cells(start_row=row - 1, start_column=4, end_row=row - 1, end_column=last)

    def inp_row(label: str, value: Any, basis: str, fmt: Optional[str]) -> Tuple[int, int]:
        nonlocal row
        sh.put(row, 2, label, font=x._FONT_BODY_BOLD)
        sh.inp(row, 3, value, fmt)
        sh.merged(row, 4, last, basis)
        ref = (3, row)
        row += 1
        return ref

    goal_ref = inp_row(
        f"Goal ({unit})",
        gp["goal"],
        "The client's stated goal: the total of the site goals when given, else the hire volume.",
        ints,
    )
    bud_ref = inp_row(
        "Planned budget",
        round(gp["budget"], 2),
        "The plan's total campaign budget.",
        cur_fmt,
    )
    plo_ref = inp_row(
        f"Projected {unit}, low",
        round(proj["low"]),
        "At the planned budget, at the industry-average cost (conservative end).",
        ints,
    )
    phi_ref = inp_row(
        f"Projected {unit}, high",
        round(proj["high"]),
        "At the planned budget, at the plan-efficiency cost (optimistic end).",
        ints,
    )
    rlo_ref = inp_row(
        "Required budget, low",
        req["low"],
        f"Smallest budget at which the optimistic end reaches the goal (the plan's own projection, inverted).",
        cur_fmt,
    )
    rhi_ref = inp_row(
        "Required budget, high",
        req["high"],
        f"Smallest budget at which the conservative end reaches the goal.",
        cur_fmt,
    )
    weeks = gp.get("weeks")
    wk_ref = inp_row(
        "Campaign weeks",
        weeks,
        "From the plan's campaign length; blank when none was stated.",
        "0.0",
    )
    row += 1

    goal, bud = _a(*goal_ref), _a(*bud_ref)
    plo, phi = _a(*plo_ref), _a(*phi_ref)
    rlo, rhi = _a(*rlo_ref), _a(*rhi_ref)
    wk = _a(*wk_ref)

    row = sh.sub(row, "Result", last)
    row = sh.header(row, ["Measure", "Low", "High", "Reads as"], 2)
    ws.merge_cells(start_row=row - 1, start_column=5, end_row=row - 1, end_column=last)
    short_row = row
    sh.put(row, 2, f"Shortfall ({unit})", font=x._FONT_BODY_BOLD)
    sh.formula(row, 3, f"=MAX(0,{goal}-{phi})", short["low"], fmt=ints)
    sh.formula(row, 4, f"=MAX(0,{goal}-{plo})", short["high"], fmt=ints)
    sh.merged(
        row,
        5,
        last,
        "Low: if the plan runs at plan efficiency. High: if it runs at the industry average.",
    )
    row += 1
    sh.put(row, 2, "Budget gap to the goal", font=x._FONT_BODY_BOLD)
    sh.formula(row, 3, f"=MAX(0,{rlo}-{bud})", short["budget_gap_low"], fmt=cur_fmt)
    sh.formula(row, 4, f"=MAX(0,{rhi}-{bud})", short["budget_gap_high"], fmt=cur_fmt)
    sh.merged(row, 5, last, "Extra spend needed on top of the planned budget.")
    row += 1
    sh.put(row, 2, "Status", font=x._FONT_BODY_BOLD)
    status_word = {"meets": "COVERED", "borderline": "NOT ASSURED", "short": "SHORT"}[
        gp["status"]
    ]
    sh.formula(
        row,
        3,
        f'=IF({plo}>={goal},"COVERED",IF({phi}>={goal},"NOT ASSURED","SHORT"))',
        status_word,
        font=x._FONT_BODY_BOLD,
    )
    sh.merged(
        row,
        4,
        last,
        "COVERED: even the conservative end reaches the goal. NOT ASSURED: only the "
        "optimistic end does. SHORT: neither does.",
    )
    row += 2

    row = sh.sub(row, "Decision options", last)
    row = sh.header(
        row,
        [
            "Option",
            "Budget, low",
            "Budget, high",
            f"Delivers ({unit}), low",
            f"Delivers ({unit}), high",
            "Short of goal at the deadline, low",
            "Short of goal at the deadline, high",
            "What it means",
        ],
        2,
    )
    a, b, c = gp["options"]
    sh.put(row, 2, "A. Full objective", font=x._FONT_BODY_BOLD)
    sh.formula(row, 3, f"={rlo}", a["budget_low"], fmt=cur_fmt)
    sh.formula(row, 4, f"={rhi}", a["budget_high"], fmt=cur_fmt)
    sh.formula(row, 5, f"={goal}", a["delivers_low"], fmt=ints)
    sh.formula(row, 6, f"={goal}", a["delivers_high"], fmt=ints)
    sh.formula(row, 7, "=0", 0, fmt=ints)
    sh.formula(row, 8, "=0", 0, fmt=ints)
    sh.put(row, 9, a["summary"])
    ws.row_dimensions[row].height = 58
    row += 1
    sh.put(row, 2, "B. Current constraint", font=x._FONT_BODY_BOLD)
    sh.formula(row, 3, f"={bud}", b["budget_low"], fmt=cur_fmt)
    sh.formula(row, 4, f"={bud}", b["budget_high"], fmt=cur_fmt)
    sh.formula(row, 5, f"={plo}", b["delivers_low"], fmt=ints)
    sh.formula(row, 6, f"={phi}", b["delivers_high"], fmt=ints)
    sh.formula(row, 7, f"=C{short_row}", short["low"], fmt=ints)
    sh.formula(row, 8, f"=D{short_row}", short["high"], fmt=ints)
    sh.put(row, 9, b["summary"])
    ws.row_dimensions[row].height = 58
    row += 1
    sh.put(row, 2, f"C. {c['title']}", font=x._FONT_BODY_BOLD)
    sh.formula(row, 3, f"={rlo}", c["budget_low"], fmt=cur_fmt)
    sh.formula(row, 4, f"={rhi}", c["budget_high"], fmt=cur_fmt)
    sh.formula(row, 5, f"={goal}", c["delivers_low"], fmt=ints)
    sh.formula(row, 6, f"={goal}", c["delivers_high"], fmt=ints)
    sh.formula(row, 7, f"=C{short_row}", short["low"], fmt=ints)
    sh.formula(row, 8, f"=D{short_row}", short["high"], fmt=ints)
    sh.put(row, 9, c["summary"])
    ws.row_dimensions[row].height = 86
    row += 1
    if c.get("kind") == "extend_deadline" and c.get("viable", True):
        row += 1
        row = sh.header(
            row,
            [
                "Option C: window at today's weekly spend",
                "Weeks to the goal, low",
                "Weeks to the goal, high",
                "Weeks added, low",
                "Weeks added, high",
            ],
            2,
        )
        sh.put(row, 2, "Weeks (current window in the inputs above)", font=x._FONT_BODY_BOLD)
        sh.formula(row, 3, f"={wk}*{rlo}/{bud}", c["weeks_low"], fmt="0.0")
        sh.formula(row, 4, f"={wk}*{rhi}/{bud}", c["weeks_high"], fmt="0.0")
        sh.formula(row, 5, f"=MAX(0,C{row}-{wk})", c["extra_weeks_low"], fmt="0.0")
        sh.formula(row, 6, f"=MAX(0,D{row}-{wk})", c["extra_weeks_high"], fmt="0.0")
        row += 1
    row = sh.note(
        row,
        "The option text describes the plan as generated; the numbers beside it recalculate when an input above changes.",
        last,
    )
    row += 1

    sites = gp.get("sites") or []
    if sites:
        row = sh.sub(row, "Per-site goal vs projected", last)
        goals_given = any(r.get("goal") is not None for r in sites)
        row = sh.header(
            row,
            [
                "Site",
                f"Site goal ({unit})",
                "Fair share of spend",
                "Budget",
                f"Projected ({unit}), low",
                f"Projected ({unit}), high",
                "Gap to goal, low",
                "Gap to goal, high",
            ],
            2,
        )
        first = row
        for i, r in enumerate(sites):
            alt = x._FILL_BLUE_PALE if i % 2 else x._FILL_WHITE
            sh.put(row, 2, r["site"], fill=alt)
            if r.get("goal") is not None:
                sh.inp(row, 3, r["goal"], ints)
            else:
                sh.put(row, 3, "no goal set", fill=alt, align=x._ALIGN_CENTER)
            sh.inp(row, 4, round(r["share"], 4), pct)
            sh.formula(row, 5, f"={bud}*D{row}", r["budget"], fmt=cur_fmt, fill=alt)
            sh.formula(row, 6, f"={plo}*D{row}", r["projected_low"], fmt="#,##0.0", fill=alt)
            sh.formula(row, 7, f"={phi}*D{row}", r["projected_high"], fmt="#,##0.0", fill=alt)
            if r.get("goal") is not None:
                sh.formula(
                    row, 8, f"=MAX(0,C{row}-G{row})", r["gap_low"], fmt="#,##0.0", fill=alt
                )
                sh.formula(
                    row, 9, f"=MAX(0,C{row}-F{row})", r["gap_high"], fmt="#,##0.0", fill=alt
                )
            else:
                sh.put(row, 8, "n/a", fill=alt, align=x._ALIGN_CENTER)
                sh.put(row, 9, "n/a", fill=alt, align=x._ALIGN_CENTER)
            row += 1
        last_row = row - 1
        sh.put(row, 2, "Total", font=x._FONT_BODY_BOLD, fill=x._FILL_OFF_WHITE)
        tot_goal = sum(r["goal"] or 0 for r in sites)
        sh.formula(row, 3, f"=SUM(C{first}:C{last_row})", tot_goal, fmt=ints, font=x._FONT_BODY_BOLD, fill=x._FILL_OFF_WHITE)
        sh.formula(row, 4, f"=SUM(D{first}:D{last_row})", sum(round(r["share"], 4) for r in sites), fmt=pct, font=x._FONT_BODY_BOLD, fill=x._FILL_OFF_WHITE)
        sh.formula(row, 5, f"=SUM(E{first}:E{last_row})", sum(r["budget"] for r in sites), fmt=cur_fmt, font=x._FONT_BODY_BOLD, fill=x._FILL_OFF_WHITE)
        sh.formula(row, 6, f"=SUM(F{first}:F{last_row})", sum(r["projected_low"] for r in sites), fmt="#,##0.0", font=x._FONT_BODY_BOLD, fill=x._FILL_OFF_WHITE)
        sh.formula(row, 7, f"=SUM(G{first}:G{last_row})", sum(r["projected_high"] for r in sites), fmt="#,##0.0", font=x._FONT_BODY_BOLD, fill=x._FILL_OFF_WHITE)
        sh.formula(
            row,
            9,
            f'=IF(ABS(D{row}-1)<0.0005,"OK","SHARES MUST ADD TO 100%")',
            "OK" if abs(sum(round(r["share"], 4) for r in sites) - 1) < 0.0005 else "SHARES MUST ADD TO 100%",
            font=x._FONT_BODY_BOLD,
            fill=x._FILL_OFF_WHITE,
        )
        row += 1
        row = sh.note(
            row,
            "Fair share follows the site goals (each site's share of the goals), the same shares the Weekly Pacing "
            "site x week split uses. Shares are editable and must add to 100%."
            if goals_given
            else "No site goals were set: shares follow the plan's location weights.",
            last,
        )
        row += 1

    for n_ in gp.get("notes") or []:
        row = sh.note(row, str(n_), last)
    row = sh.note(row, str(gp["assumptions"]["basis"]), last)


def append_sheets(wb: Any, data: dict, today: Optional[datetime.date] = None) -> bool:
    """Add the five enterprise sheets to ``wb``.  Called by excel_v2 only
    when :func:`enabled`; returns False (and adds nothing) otherwise.  Each
    sheet is isolated: one failing cannot block the others or the workbook."""
    if not enabled(data):
        return False
    import excel_v2 as x

    model = build_model(data, today)
    tab = {
        SHEET_PACING: getattr(x, "NAVY", None),
        SHEET_TIERS: getattr(x, "SAPPHIRE", None),
        SHEET_TRACKING: getattr(x, "TEAL_HEX", None),
        SHEET_FEASIBILITY: getattr(x, "MAGENTA_HEX", None),
        SHEET_JOBAD: getattr(x, "PURPLE_LIGHT_HEX", None),
    }
    x._TAB_COLORS.update({k: v for k, v in tab.items() if v})

    pacing: Optional[Dict[str, Any]] = None
    tracking: Optional[Dict[str, Any]] = None
    # Build order follows the dependencies (tiers read pacing, feasibility
    # reads tracking); sheet ORDER in the workbook stays the product order.
    goal_plan = goal_plan_of(data)
    titles = ((SHEET_GOALPLAN,) if goal_plan else ()) + SHEET_TITLES
    if goal_plan:
        x._TAB_COLORS[SHEET_GOALPLAN] = getattr(x, "PURPLE_LIGHT_HEX", None) or tab[SHEET_PACING]
    sheets = {t: wb.create_sheet() for t in titles}
    for title, ws in sheets.items():
        ws.title = title
    plan = (
        (SHEET_PACING, lambda ws: _build_pacing(ws, data, model, x)),
        (SHEET_TIERS, lambda ws: _build_tiers(ws, data, model, x, pacing)),
        (SHEET_TRACKING, lambda ws: _build_tracking(ws, data, model, x)),
        (
            SHEET_FEASIBILITY,
            lambda ws: _build_feasibility(ws, data, model, x, tracking),
        ),
        (SHEET_JOBAD, lambda ws: _build_jobad(ws, data, model, x)),
    )
    if goal_plan:
        plan = ((SHEET_GOALPLAN, lambda ws: _build_goalplan(ws, data, goal_plan, x)),) + plan
    for title, build in plan:
        ws = sheets[title]
        try:
            result = build(ws)
            if title == SHEET_PACING:
                pacing = result
            elif title == SHEET_TRACKING:
                tracking = result
        except Exception as exc:  # noqa: BLE001 - isolate each sheet
            logger.error("Enterprise sheet %s failed: %s", title, exc, exc_info=True)
            for rng in list(ws.merged_cells.ranges):
                ws.unmerge_cells(str(rng))
            for r in ws.iter_rows():
                for c in r:
                    c.value = None
            ws.cell(row=2, column=2, value=f"Could not build the {title} sheet: {exc}")
            if title == SHEET_PACING:
                pacing = None
    return True


# ---------------------------------------------------------------------------
# Deck appendix (3 slides)
# ---------------------------------------------------------------------------
_L = 0.55  # left margin (in)
_W = 12.23  # content width (in)


def _drop_last_slide(prs: Any) -> None:
    """Remove the most recently added slide (a builder failed mid-slide)."""
    lst = prs.slides._sldIdLst
    last = lst[-1]
    prs.part.drop_rel(last.rId)
    lst.remove(last)


def _slide_base(prs: Any, ppt: Any, title: str, subtitle: str) -> Any:
    from pptx.util import Inches

    slide = prs.slides.add_slide(prs.slide_layouts[6])
    today = datetime.date.today().strftime("%B %d, %Y")
    ppt._add_filled_rect(
        slide, Inches(0), Inches(0), ppt.SLIDE_WIDTH, ppt.SLIDE_HEIGHT, ppt.OFF_WHITE
    )
    ppt._add_top_band(slide, title, today)
    ppt._add_textbox(
        slide,
        Inches(_L),
        Inches(0.92),
        Inches(_W),
        Inches(0.45),
        text=subtitle,
        font_size=13,
        bold=True,
        color=ppt.NAVY,
    )
    return slide


def _card(
    slide: Any, ppt: Any, x: float, y: float, w: float, h: float, heading: str
) -> None:
    from pptx.util import Inches

    ppt._add_filled_rect(
        slide, Inches(x), Inches(y), Inches(w), Inches(h), ppt.WARM_GRAY
    )
    ppt._add_filled_rect(
        slide,
        Inches(x + 0.012),
        Inches(y + 0.012),
        Inches(w - 0.024),
        Inches(h - 0.024),
        ppt.WHITE,
    )
    ppt._add_textbox(
        slide,
        Inches(x + 0.2),
        Inches(y + 0.1),
        Inches(w - 0.4),
        Inches(0.34),
        text=heading,
        font_size=12,
        bold=True,
        color=ppt.NAVY,
    )


def _line(
    slide: Any,
    ppt: Any,
    x: float,
    y: float,
    w: float,
    h: float,
    text: str,
    size: float = 10,
    bold: bool = False,
    color: Any = None,
    body: bool = True,
    center: bool = False,
) -> None:
    from pptx.enum.text import PP_ALIGN
    from pptx.util import Inches

    _box, tf = ppt._add_textbox(
        slide,
        Inches(x),
        Inches(y),
        Inches(w),
        Inches(h),
        text=text,
        font_size=size,
        bold=bold,
        color=color or ppt.DARK_TEXT,
        alignment=PP_ALIGN.CENTER if center else PP_ALIGN.LEFT,
    )
    if body:
        ppt._set_body_font(tf)


def _table(
    slide: Any,
    ppt: Any,
    x: float,
    y: float,
    widths: List[float],
    headers: List[str],
    rows: List[List[str]],
    row_h: float = 0.5,
) -> float:
    """Shape-built table (indigo header, zebra rows); returns the bottom y."""
    from pptx.util import Inches

    total = sum(widths)
    hdr_h = 0.4
    ppt._add_filled_rect(
        slide, Inches(x), Inches(y), Inches(total), Inches(hdr_h), ppt.NAVY
    )
    cx = x
    for h, w in zip(headers, widths):
        _line(
            slide,
            ppt,
            cx + 0.12,
            y,
            w - 0.24,
            hdr_h,
            h,
            size=10,
            bold=True,
            color=ppt.WHITE,
            body=False,
        )
        _fix_anchor(slide, ppt)
        cx += w
    for i, row in enumerate(rows):
        ry = y + hdr_h + i * row_h
        ppt._add_filled_rect(
            slide,
            Inches(x),
            Inches(ry),
            Inches(total),
            Inches(row_h),
            ppt.LAVENDER_50 if i % 2 == 0 else ppt.WHITE,
        )
        cx = x
        for j, (cell, w) in enumerate(zip(row, widths)):
            _line(
                slide,
                ppt,
                cx + 0.12,
                ry,
                w - 0.24,
                row_h,
                cell,
                size=10 if j else 10,
                bold=(j == 0),
                color=ppt.NAVY if j == 0 else ppt.DARK_TEXT,
                body=(j != 0),
            )
            _fix_anchor(slide, ppt)
            cx += w
    return y + hdr_h + len(rows) * row_h


def _fix_anchor(slide: Any, ppt: Any) -> None:
    """Centre the text of the most recent box vertically in its frame."""
    try:
        shape = slide.shapes[-1]
        shape.text_frame._txBody.bodyPr.set("anchor", "ctr")
    except Exception:  # noqa: BLE001 - cosmetic
        pass


def _clip(text: Any, limit: int) -> str:
    """``text`` cut at a word boundary to ``limit`` characters with one ellipsis."""
    t = str(text or "").strip()
    if len(t) <= limit:
        return t
    hard = t[: limit - 1]
    cut = hard.rsplit(" ", 1)[0]
    if len(cut) < limit * 0.6:  # a word boundary that loses most of the name
        cut = hard
    return cut.rstrip(" ,;:-/(") + "…"


def _money(ppt: Any, value: float) -> str:
    return ppt._fmt_currency_whole(value)


def _slide_pacing(prs: Any, data: dict, model: Dict[str, Any], ppt: Any) -> None:
    n = len(model["weeks"])
    eff = model["effective_weeks"] or 1.0
    steps = ladder(model)
    cpa = model["cpa_range"]
    rec = steps[1]
    sub = f"Appendix: {_money(ppt, steps[0]['weekly'])} to {_money(ppt, steps[2]['weekly'])} a week, over {n} fiscal weeks"
    slide = _slide_base(prs, ppt, "Weekly Pacing & Budget Ladder", sub)
    widths = [2.5, 2.0, 2.0, 2.4, 3.33] if cpa else [2.8, 2.4, 2.4, 4.63]
    headers = (
        ["Step", "Avg weekly spend", "Campaign spend"]
        + (["Apply starts / week"] if cpa else [])
        + ["Basis"]
    )
    rows = []
    for s in steps:
        row = [s["step"], _money(ppt, s["weekly"]), _money(ppt, s["campaign"])]
        if cpa:
            row.append(format_range(s["starts_low"] / eff, s["starts_high"] / eff))
        row.append(s["basis"])
        rows.append(row)
    bottom = _table(slide, ppt, _L, 1.5, widths, headers, rows, row_h=0.62)

    y = bottom + 0.3
    cfg_note = (
        f"Fiscal week {_weekday_name(WEEKDAYS.index(model['settings']['week_start']))} to "
        f"{_weekday_name(WEEKDAYS.index(model['settings']['week_start']) - 1)}; week 1 contains the campaign start "
        f"({model['start'].strftime('%d %b %Y')})"
        + (
            f", and {sum(1 for w in model['weeks'] if w['partial'])} partial week(s) are weighted by active days."
            if any(w["partial"] for w in model["weeks"])
            else "."
        )
    )
    _card(slide, ppt, _L, y, 5.9, 2.2, "Status bands (weekly goal)")
    from pptx.util import Inches

    bands = (
        (
            ppt.GREEN,
            f"GREEN: at or above {DEFAULT_GREEN:.0%} of the weekly goal",
            "22C55E",
        ),
        (
            ppt.AMBER_TEXT,
            f"AMBER: {DEFAULT_AMBER:.0%} to {DEFAULT_GREEN:.0%}",
            "F59E0B",
        ),
        (ppt.RED_TEXT, f"RED: below {DEFAULT_AMBER:.0%}", "EF4444"),
    )
    for i, (col, text, hexcode) in enumerate(bands):
        by = y + 0.62 + i * 0.4
        ppt._add_filled_rect(
            slide,
            Inches(_L + 0.25),
            Inches(by + 0.05),
            Inches(0.22),
            Inches(0.22),
            ppt._rgb(hexcode),
        )
        _line(slide, ppt, _L + 0.6, by, 5.0, 0.3, text, size=10, bold=True, color=col)
    _card(slide, ppt, _L + 6.13, y, 6.1, 2.2, "Pause and reallocate")
    rules = (
        "Two RED days in a row: stop adding spend to that publisher.",
        "A RED week: pause the weakest publisher, move its budget to Tier 1 publishers that are GREEN.",
        "AMBER: hold budget and fix the cause first. Validate tracking before any move.",
    )
    for i, r in enumerate(rules):
        _line(slide, ppt, _L + 6.38, y + 0.55 + i * 0.52, 5.65, 0.5, r, size=10)
    foot = (
        cfg_note
        + (
            " Apply starts are estimated from the plan's cost per application."
            if cpa
            else " No cost per application in the plan, so apply starts are omitted and the bands apply to the client's weekly goal."
        )
        + " Every threshold is an editable default in the workbook's Weekly Pacing sheet: set it to the client's SLA."
    )
    _line(slide, ppt, _L, y + 2.32, _W, 0.5, foot, size=9, color=ppt.MUTED_TEXT)
    ppt._add_footer(slide, datetime.date.today().strftime("%B %d, %Y"))


def _slide_tiers(prs: Any, data: dict, model: Dict[str, Any], ppt: Any) -> None:
    from pptx.util import Inches

    pubs = tiered_publishers(model)
    n = model["effective_weeks"] or 1.0
    sub = f"Appendix: {len(pubs)} funded publishers in three launch tiers; Weekly Pacing governs total spend"
    slide = _slide_base(prs, ppt, "Publisher Tiers & Gates", sub)
    heads = (
        ("Tier 1", "Launch in week 1"),
        ("Tier 2", "Launch after Gate 1"),
        ("Tier 3", "Test after Gate 2"),
    )
    col_w, gap = 3.93, 0.22
    for i, (tier, rule) in enumerate(heads):
        x = _L + i * (col_w + gap)
        _card(slide, ppt, x, 1.5, col_w, 2.3, f"{tier}: {rule}")
        members = [p for p in pubs if p["tier"] == tier]
        shown = members[:4]
        if not members:
            _line(
                slide,
                ppt,
                x + 0.2,
                2.0,
                col_w - 0.4,
                0.3,
                "No publisher at this tier on the default cut-offs.",
                size=10,
                color=ppt.MUTED_TEXT,
            )
        for k, p in enumerate(shown):
            weekly_min = _xl_round(p["budget"] / n * DEFAULT_MIN_TEST_FACTOR)
            _line(
                slide,
                ppt,
                x + 0.2,
                1.98 + k * 0.36,
                2.35,
                0.32,
                _clip(p["name"], 26),
                size=10,
                bold=True,
                color=ppt.NAVY,
                body=False,
            )
            _line(
                slide,
                ppt,
                x + 2.6,
                1.98 + k * 0.36,
                1.25,
                0.32,
                f"min {_money(ppt, weekly_min)}/wk",
                size=9,
                color=ppt.MUTED_TEXT,
            )
        if len(members) > len(shown):
            _line(
                slide,
                ppt,
                x + 0.2,
                1.98 + len(shown) * 0.36 + 0.05,
                col_w - 0.4,
                0.3,
                f"+{len(members) - len(shown)} more in the workbook",
                size=9,
                color=ppt.MUTED_TEXT,
            )
    _card(slide, ppt, _L, 4.05, 6.0, 2.8, "Switch triggers (defaults)")
    trig = (
        f"Tier 1 publisher below {DEFAULT_AMBER:.0%} of goal for {DEFAULT_REALLOCATE_DAYS} days: move its budget to Tier 1 publishers that are GREEN.",
        f"Tier 2 publisher at or above {DEFAULT_PROMOTE_PCT:.0%} of goal for {DEFAULT_PROMOTE_DAYS} days: fund it as Tier 1.",
        f"Any publisher below {DEFAULT_AMBER:.0%} for {DEFAULT_PAUSE_DAYS} days: pause, reallocate, check tracking.",
    )
    for i, t in enumerate(trig):
        _line(slide, ppt, _L + 0.2, 4.55 + i * 0.72, 5.6, 0.62, t, size=10)
    _card(slide, ppt, _L + 6.23, 4.05, 6.0, 2.8, "Performance gates")
    gates = (
        "Gate 0, before launch: every link records a test apply in the system of record.",
        f"Gate 1, end of week 1: Tier 1 at {DEFAULT_AMBER:.0%} of goal or better, then launch Tier 2.",
        f"Gate 2, end of week 2: Tier 1 and 2 at {DEFAULT_AMBER:.0%}, then test Tier 3.",
        f"Gate 3, week 4 on: total at {DEFAULT_GREEN:.0%} of goal every week.",
    )
    for i, g in enumerate(gates):
        _line(slide, ppt, _L + 6.43, 4.55 + i * 0.55, 5.6, 0.46, g, size=10)
    ppt._add_footer(slide, datetime.date.today().strftime("%B %d, %Y"))


def _slide_tracking(prs: Any, data: dict, model: Dict[str, Any], ppt: Any) -> None:
    from pptx.util import Inches

    spec = tracking_spec(model)
    summ = feasibility_summary(model)
    counts = summ["counts"]
    cfg = model["settings"]
    sub = (
        "Appendix: how every apply is counted, and whether the plan can run as written"
    )
    slide = _slide_base(prs, ppt, "Tracking & Feasibility", sub)
    _card(slide, ppt, _L, 1.5, 6.0, 5.35, "Tracking spec")
    sor = cfg["system_of_record"] or DEFAULT_SYSTEM_OF_RECORD
    items = (
        ("utm_source", f"publisher slug, e.g. {spec['example_source']}"),
        ("utm_medium", f"channel type, e.g. {spec['example_medium']}"),
        ("utm_campaign", f"plan, site, week, e.g. {spec['example_campaign']}"),
        ("Campaign ID", spec["example_id"]),
        ("System of record", re.sub(r"(\S{34})(?=\S)", r"\1 ", _clip(sor, 80))),
        (
            "Reconciliation",
            f"systems differ by more than {DEFAULT_RECON_TOLERANCE:.0%} for two days: freeze budget moves, compare by campaign ID",
        ),
        ("Before launch", "test apply on every link, seen in the system of record"),
        (
            "After launch",
            f"re-check at 24 hours; fix tracking faults within {DEFAULT_FIX_WINDOW_HOURS} hours",
        ),
    )
    y = 2.0
    for label, text in items:
        _line(
            slide,
            ppt,
            _L + 0.2,
            y,
            1.6,
            0.5,
            label,
            size=10,
            bold=True,
            color=ppt.NAVY,
            body=False,
        )
        _line(slide, ppt, _L + 1.85, y, 4.0, 0.55, text, size=10)
        y += 0.58
    x0 = _L + 6.23
    _card(slide, ppt, x0, 1.5, 6.0, 5.35, "Feasibility check")
    chip_w = 1.3
    chips = (
        ("PASS", "15803D", "F0FDF4"),
        ("CHECK", "B45309", "FEF3C7"),
        ("FAIL", "B91C1C", "FEF2F2"),
        ("CONFIRM", "5A54BE", "ECEAF7"),
    )
    for i, (word, text_hex, fill_hex) in enumerate(chips):
        cx = x0 + 0.2 + i * (chip_w + 0.12)
        ppt._add_rounded_rect(
            slide,
            Inches(cx),
            Inches(2.0),
            Inches(chip_w),
            Inches(0.95),
            ppt._rgb(fill_hex),
        )
        _line(
            slide,
            ppt,
            cx,
            2.03,
            chip_w,
            0.5,
            str(counts.get(word, 0)),
            size=20,
            bold=True,
            color=ppt._rgb(text_hex),
            body=False,
            center=True,
        )
        _line(
            slide,
            ppt,
            cx,
            2.55,
            chip_w,
            0.3,
            word,
            size=9,
            bold=True,
            color=ppt._rgb(text_hex),
            center=True,
        )
    verdict = (
        "NOT READY"
        if counts.get("FAIL")
        else (
            "READY AFTER CONFIRMATIONS"
            if counts.get("CHECK") or counts.get("CONFIRM")
            else "READY"
        )
    )
    _line(
        slide,
        ppt,
        x0 + 0.2,
        3.15,
        5.6,
        0.32,
        f"Gate result: {verdict}",
        size=11,
        bold=True,
        color=ppt.NAVY,
        body=False,
    )
    open_rows = [r for r in summ["rows"] if r["status"] != "PASS"][:5]
    yy = 3.6
    for r in open_rows:
        _line(
            slide,
            ppt,
            x0 + 0.2,
            yy,
            1.0,
            0.4,
            r["status"],
            size=9,
            bold=True,
            color=ppt.NAVY,
            body=False,
        )
        _line(slide, ppt, x0 + 1.2, yy, 4.6, 0.58, r["check"], size=10)
        yy += 0.62
    if not open_rows:
        _line(
            slide,
            ppt,
            x0 + 0.2,
            yy,
            5.6,
            0.4,
            "Every check passes on the plan's own data.",
            size=10,
        )
    ppt._add_footer(slide, datetime.date.today().strftime("%B %d, %Y"))


def _slide_goalplan(prs: Any, data: dict, gp: Dict[str, Any], ppt: Any) -> None:
    unit = gp["unit"]
    sym = (gp.get("currency") or {}).get("symbol") or "$"
    status = gp.get("status")
    slide = _slide_base(
        prs,
        ppt,
        "Goal Plan: What It Takes",
        _clip(f"Appendix: {gp['text']['callout']}", 150),
    )
    if status not in ("meets", "borderline", "short"):
        _card(slide, ppt, _L, 1.6, _W, 1.6, "Goal not priced")
        _line(slide, ppt, _L + 0.25, 2.1, _W - 0.5, 1.0, str(gp["text"]["callout"]), size=12)
        ppt._add_footer(slide, datetime.date.today().strftime("%B %d, %Y"))
        return

    def m(v: Any) -> str:
        return f"{sym}{float(v):,.0f}"

    def mr(lo: Any, hi: Any) -> str:
        a, b = m(lo), m(hi)
        return a if a == b else f"{a}-{b}"

    def cr(lo: Any, hi: Any) -> str:
        a, b = f"{float(lo):,.0f}", f"{float(hi):,.0f}"
        return a if a == b else f"{a}-{b}"

    a, b, c = gp["options"]
    short = gp["shortfall"]
    rows = [
        [
            "A. Full objective",
            mr(a["budget_low"], a["budget_high"]),
            demand_planning.count_phrase(gp["goal"], gp["goal"], gp["kind"]),
            "none",
            _clip(
                "Fund the goal."
                + (
                    f" {mr(a['extra_budget_low'], a['extra_budget_high'])} more than planned."
                    if a["extra_budget_high"] > 0
                    else " The planned budget already covers it."
                ),
                70,
            ),
        ],
        [
            "B. Current constraint",
            m(gp["budget"]),
            demand_planning.count_phrase(b["delivers_low"], b["delivers_high"], gp["kind"]),
            demand_planning.count_phrase(short["low"], short["high"], gp["kind"])
            if short["high"] > 0
            else "none",
            _clip("Hold the budget; accept what it buys.", 70),
        ],
    ]
    if c.get("kind") == "extend_deadline" and short["high"] > 0:
        if not c.get("viable", True):
            c_title = "C. Extend the deadline"
            c_text = (
                f"Not viable alone: about {c['weeks_high']:.0f} weeks at today's "
                f"weekly spend (window is {c['weeks_now']:g}). Add budget or lower the goal."
            )
        elif c.get("fits_now"):
            c_title = "C. Extend the deadline"
            c_text = (
                f"Fits the {c['weeks_now']:g}-week window at plan efficiency; about "
                f"{c['weeks_high']:.0f} weeks at the industry-average cost."
            )
        else:
            c_title = "C. Extend the deadline"
            c_text = (
                f"Same weekly spend, window grows to {c['weeks_low']:.0f}-{c['weeks_high']:.0f} "
                f"weeks (now {c['weeks_now']:g})."
                if abs(c["weeks_high"] - c["weeks_low"]) >= 0.5
                else f"Same weekly spend, window grows to {c['weeks_high']:.0f} weeks (now {c['weeks_now']:g})."
            )
        rows.append(
            [
                c_title,
                mr(c["budget_low"], c["budget_high"]),
                demand_planning.count_phrase(gp["goal"], gp["goal"], gp["kind"]),
                f"{demand_planning.count_phrase(short['low'], short['high'], gp['kind'])} at today's deadline",
                _clip(c_text, 110),
            ]
        )
    elif c.get("kind") == "second_flight" and short["high"] > 0:
        rows.append(
            [
                "C. Second flight",
                mr(c["budget_low"], c["budget_high"]),
                demand_planning.count_phrase(gp["goal"], gp["goal"], gp["kind"]),
                f"{demand_planning.count_phrase(short['low'], short['high'], gp['kind'])} until the second flight",
                _clip(
                    f"Fund {mr(c['extra_budget_low'], c['extra_budget_high'])} more in a second flight.",
                    90,
                ),
            ]
        )
    else:
        rows.append(
            [
                "C. Alternative",
                "not needed",
                demand_planning.count_phrase(gp["goal"], gp["goal"], gp["kind"]),
                "none",
                "The planned budget covers the goal.",
            ]
        )
    bottom = _table(
        slide,
        ppt,
        _L,
        1.5,
        [2.3, 2.2, 1.9, 2.3, 3.53],
        ["Option", "Budget", "Delivers", "Short of goal", "What it means"],
        rows,
        row_h=0.62,
    )
    y = bottom + 0.25
    site_rows = gp.get("sites") or []
    if site_rows:
        shown = site_rows[:4]
        body = []
        for r in shown:
            goal_txt = f"{r['goal']:,}" if r.get("goal") is not None else "no goal"
            gap_txt = (
                "covered"
                if r.get("gap_high") is not None and r["gap_high"] <= 0
                else (
                    f"{cr(r['gap_low'], r['gap_high'])} short"
                    if r.get("gap_high") is not None
                    else "n/a"
                )
            )
            body.append(
                [
                    _clip(r["site"], 28),
                    goal_txt,
                    f"{r['share']:.0%}",
                    f"{cr(r['projected_low'], r['projected_high'])}",
                    gap_txt,
                ]
            )
        bottom = _table(
            slide,
            ppt,
            _L,
            y,
            [3.4, 1.9, 1.9, 2.5, 2.53],
            ["Site", f"Goal ({unit})", "Share of spend", f"Projected ({unit})", "Gap to goal"],
            body,
            row_h=0.36,
        )
        more = len(site_rows) - len(shown)
        if more > 0:
            _line(
                slide,
                ppt,
                _L,
                bottom + 0.04,
                _W,
                0.28,
                f"{more} more site(s) in the workbook's Goal Plan sheet.",
                size=9,
                color=ppt.MUTED_TEXT,
            )
            bottom += 0.3
        y = bottom + 0.2
    foot = (
        "Estimates, not forecasts: the plan's own projection inverted (industry cost per hire, no client hire history). "
        "Required budget runs from the plan-efficiency cost (low) to the industry-average cost (high). "
        "The workbook's Goal Plan sheet recalculates from editable inputs."
    )
    _line(slide, ppt, _L, min(y, 6.45), _W, 0.55, foot, size=9, color=ppt.MUTED_TEXT)
    ppt._add_footer(slide, datetime.date.today().strftime("%B %d, %Y"))


def add_deck_appendix(prs: Any, data: dict, ppt: Any) -> int:
    """Append the three enterprise slides to ``prs`` (``ppt`` is the
    ppt_generator module, passed in so its drawing helpers are reused).
    Returns the number of slides added; 0 when the pack is off."""
    if not enabled(data):
        return 0
    model = build_model(data)
    added = 0
    builders: List[Any] = [_slide_pacing, _slide_tiers, _slide_tracking]
    goal_plan = goal_plan_of(data)
    if goal_plan:
        builders.append(
            lambda prs_, data_, model_, ppt_: _slide_goalplan(
                prs_, data_, goal_plan, ppt_
            )
        )
    for builder in builders:
        before = len(prs.slides)
        try:
            builder(prs, data, model, ppt)
            added += 1
        except (
            Exception
        ) as exc:  # noqa: BLE001 - an appendix slide never blocks the deck
            logger.error(
                "Enterprise slide %s failed: %s",
                getattr(builder, "__name__", "goal plan"),
                exc,
                exc_info=True,
            )
            while len(prs.slides) > before:
                _drop_last_slide(prs)
    return added
