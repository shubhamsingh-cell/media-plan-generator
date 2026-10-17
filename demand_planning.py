"""demand_planning.py -- plan BACKWARD from a hiring goal.

``plan_projection.project_plan`` answers "what does this budget buy?".  Enterprise
hourly clients ask the inverse: "we need N hires (or N apply starts) by a date --
what does that cost, and what do we do when the budget does not cover it?".

This module inverts the SAME engine instead of re-deriving a second one:

* :func:`required_budget` bisects ``project_plan`` over the budget (the projected
  counts are non-decreasing in budget, so the smallest budget that reaches the
  goal is found exactly, to the dollar).  The low end of the answer is the
  smallest budget at which the OPTIMISTIC end of the projection range reaches
  the goal (plan-efficiency cost per hire); the high end is the smallest budget
  at which the CONSERVATIVE end does (industry-average cost per hire).  A goal
  that sits between the two is "possible, not assured" -- the range says so.
* :func:`build_goal_plan` turns a goal + the actual budget into the shortfall
  and three decision options: (A) the full objective, (B) the current
  constraint, (C) an alternative -- extend the deadline at the current weekly
  spend, or add a second flight when no campaign length is known.
* :func:`site_weights` turns per-site hiring goals into the allocation weights
  the weekly site x week split uses (fair share = share of the goals).

Nothing here is a new benchmark.  Every figure is project_plan's own range or
arithmetic on it.  A market whose hires are not projected (no local
cost-per-hire benchmark) gets a goal plan in apply starts only, and says so.

Pure and thread-safe: no module state, no I/O of its own (the caller supplies
the ``projector``).  Never raises; a failure yields ``{"ok": False, ...}``.
"""

from __future__ import annotations

import logging
import math
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

GOAL_HIRES = "hires"
GOAL_APPLY_STARTS = "apply_starts"

MAX_GOAL = 100000  # the same cap the hire volume field has
MAX_SITE_GOALS = 40
MAX_SITE_NAME_CHARS = 120

# Search limits: a goal that no budget up to this reaches is reported as
# "not reachable", never looped on.
_START_BUDGET = 1000.0
_MAX_BUDGET = 1e12
_MAX_BRACKET_STEPS = 45
_MAX_BISECT_STEPS = 60
_ABS_TOLERANCE = 1.0  # dollars

Projector = Callable[[float], Dict[str, Any]]


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def _num(value: Any) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return 0.0
    return out if math.isfinite(out) else 0.0


def _metric(proj: Dict[str, Any], kind: str) -> Optional[Tuple[float, float]]:
    """(conservative, optimistic) count of the goal's unit, or None when the
    projection carries no such figure (hires suppressed for the market)."""
    if not isinstance(proj, dict) or not proj.get("ok"):
        return None
    rng = proj.get("hires" if kind == GOAL_HIRES else "applications")
    if not isinstance(rng, dict):
        return None
    lo, hi = rng.get("low"), rng.get("high")
    if lo is None or hi is None:
        return None
    return float(lo), float(hi)


def unit_label(kind: str) -> str:
    return "hires" if kind == GOAL_HIRES else "apply starts"


def city_key(text: Any) -> str:
    """Matching key for a site: the part before the first comma, alphanumerics
    only -- the same rule the weekly pacing split uses for location weights."""
    return re.sub(r"[^a-z0-9]", "", str(text or "").split(",")[0].lower())


# --------------------------------------------------------------------------
# D1: inverting project_plan
# --------------------------------------------------------------------------


def _smallest_budget(
    reaches: Callable[[float], Optional[bool]], hint: float
) -> Tuple[Optional[float], str]:
    """Smallest budget (to ``_ABS_TOLERANCE`` dollars) at which ``reaches`` is
    true, for a predicate that is monotone in the budget.  ``(None, reason)``
    when no budget up to the cap gets there or the projection is unavailable.
    The returned budget always satisfies the predicate (the true side of the
    bracket)."""
    hi = max(_START_BUDGET, hint)
    ok = reaches(hi)
    steps = 0
    while ok is False and hi < _MAX_BUDGET and steps < _MAX_BRACKET_STEPS:
        hi *= 2.0
        ok = reaches(hi)
        steps += 1
    if ok is None:
        return None, "not_projected"
    if not ok:
        return None, "out_of_range"
    lo = 0.0
    # shrink the bracket from below as well: halve while the lower half still
    # reaches the goal, so an oversized hint costs a few calls, not a wide search
    for _ in range(_MAX_BRACKET_STEPS):
        mid = hi / 2.0
        if mid < _START_BUDGET:
            break
        r = reaches(mid)
        if r:
            hi = mid
        else:
            lo = mid
            break
    for _ in range(_MAX_BISECT_STEPS):
        if hi - lo <= _ABS_TOLERANCE:
            break
        mid = (lo + hi) / 2.0
        r = reaches(mid)
        if r:
            hi = mid
        else:
            lo = mid
    return float(math.ceil(hi)), ""


def required_budget(
    goal: float,
    kind: str,
    projector: Projector,
    *,
    hint: float = 0.0,
) -> Dict[str, Any]:
    """The budget range that reaches ``goal`` hires (or apply starts).

    ``projector(budget)`` is ``plan_projection.project_plan`` with every other
    input fixed.  Returns ``{"ok", "low", "high", "status"}``: ``low`` is the
    smallest budget at which the optimistic end of the projected range reaches
    the goal, ``high`` the smallest at which the conservative end does
    (``low <= high``).  ``status`` is "" or "not_projected" (hires suppressed
    for the market) / "out_of_range" (no budget up to the search cap reaches
    it).  Whole-dollar, rounded UP, so ``projector(low)`` really reaches it.
    """
    goal = float(goal)
    if kind not in (GOAL_HIRES, GOAL_APPLY_STARTS) or goal <= 0:
        return {"ok": False, "status": "bad_goal", "low": None, "high": None}
    cache: Dict[float, Optional[Tuple[float, float]]] = {}

    def at(budget: float) -> Optional[Tuple[float, float]]:
        key = round(float(budget), 4)
        if key not in cache:
            try:
                cache[key] = _metric(projector(key), kind)
            except Exception as exc:  # noqa: BLE001 - a projector failure is "not projected"
                logger.debug("demand_planning: projector failed: %s", exc)
                cache[key] = None
        return cache[key]

    def optimistic(b: float) -> Optional[bool]:
        m = at(b)
        return None if m is None else m[1] >= goal

    def conservative(b: float) -> Optional[bool]:
        m = at(b)
        return None if m is None else m[0] >= goal

    low, why = _smallest_budget(optimistic, hint)
    if low is None:
        return {"ok": False, "status": why, "low": None, "high": None}
    high, why = _smallest_budget(conservative, max(low, hint))
    if high is None:
        return {"ok": False, "status": why, "low": low, "high": None}
    return {"ok": True, "status": "", "low": low, "high": max(low, high)}


# --------------------------------------------------------------------------
# D2: per-site goals
# --------------------------------------------------------------------------


def site_goals_problem(raw: Any) -> Tuple[str, str]:
    """("", "") when ``raw`` is an acceptable ``site_goals`` value, else
    (code, message).  ``None`` / ``""`` / ``{}`` are fine (no site goals)."""
    if raw in (None, "", {}):
        return "", ""
    if not isinstance(raw, dict):
        return "type", "site_goals must be an object of location -> hires."
    if len(raw) > MAX_SITE_GOALS:
        return "too_many", f"site_goals has too many sites (max {MAX_SITE_GOALS})."
    total = 0
    for name, val in raw.items():
        if not isinstance(name, str) or not name.strip():
            return "name", "Every site goal needs a location name."
        if len(name.strip()) > MAX_SITE_NAME_CHARS:
            return (
                "name",
                f"A site name is too long (max {MAX_SITE_NAME_CHARS} characters).",
            )
        if isinstance(val, bool) or val is None or val == "":
            return "value", "Each site goal must be a whole number of hires."
        try:
            n = float(val)
        except (TypeError, ValueError):
            return "value", "Each site goal must be a whole number of hires."
        if not math.isfinite(n) or n != int(n):
            return "value", "Each site goal must be a whole number of hires."
        n = int(n)
        if n < 0:
            return "value", "A site goal cannot be negative."
        if n > MAX_GOAL:
            return "value", f"A site goal exceeds the maximum ({MAX_GOAL:,} hires)."
        total += n
    if total > MAX_GOAL:
        return "total", f"Site goals add up to more than {MAX_GOAL:,} hires."
    return "", ""


def clean_site_goals(raw: Any) -> Dict[str, int]:
    """Canonical ``{site: hires}`` from an already-validated value: names
    stripped, zero goals dropped, case-insensitive duplicates merged by sum."""
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, int] = {}
    seen: Dict[str, str] = {}
    for name, val in raw.items():
        try:
            n = int(float(val))
        except (TypeError, ValueError):
            continue
        text = " ".join(str(name).split())
        if not text or n <= 0:
            continue
        key = text.lower()
        if key in seen:
            out[seen[key]] += n
        else:
            seen[key] = text
            out[text] = n
    return out


def goal_for_site(site: str, goals: Dict[str, int]) -> int:
    """The goal for ``site``: exact name first, then the city part."""
    if site in goals:
        return int(goals[site])
    ck = city_key(site)
    if not ck:
        return 0
    for name, val in goals.items():
        if city_key(name) == ck:
            return int(val)
    return 0


def site_weights(
    sites: List[str],
    goals: Dict[str, int],
    *,
    total_goal: int = 0,
    fallback: Optional[List[float]] = None,
) -> Tuple[List[float], str]:
    """Allocation weights across ``sites`` that follow the hiring goals.

    A site's weight is its goal (fair share = its share of the goals).  Sites
    with no goal get nothing -- unless a plan-wide goal is larger than the sum
    of the site goals, in which case the unassigned remainder is spread over
    the goal-less sites in proportion to ``fallback`` (the plan's own location
    weights, else equal).  Returns ``(weights, basis)``; an empty ``goals`` or
    no match returns ``([], "")`` so the caller keeps its default allocation.
    """
    if not sites or not goals:
        return [], ""
    per_site = [goal_for_site(s, goals) for s in sites]
    if sum(per_site) <= 0:
        return [], ""
    weights = [float(g) for g in per_site]
    empty = [i for i, g in enumerate(per_site) if g <= 0]
    remainder = max(0, int(total_goal) - sum(per_site))
    if empty and remainder > 0:
        base = [
            max(0.0, float(fallback[i])) if fallback and i < len(fallback) else 1.0
            for i in empty
        ]
        denom = sum(base) or float(len(empty))
        for i, w in zip(empty, base):
            weights[i] = remainder * (w / denom if sum(base) else 1.0 / len(empty))
        basis = "site hiring goals (fair share of the goals; unassigned goal split by location weight)"
    elif empty:
        basis = "site hiring goals (fair share of the goals; sites without a goal get no spend)"
    else:
        basis = "site hiring goals (fair share of the goals)"
    return weights, basis


# --------------------------------------------------------------------------
# D1/D3: the goal plan (shortfall + options A / B / C)
# --------------------------------------------------------------------------


def campaign_weeks(data: Any) -> Optional[float]:
    """Campaign length in weeks from the plan's own duration fields, or None
    when the brief states none (no default is invented)."""
    if not isinstance(data, dict):
        return None
    try:
        import gold_standard

        weeks = int(gold_standard._plan_campaign_weeks(data))
        return float(weeks) if weeks > 0 else None
    except Exception as exc:  # noqa: BLE001 - length is optional context
        logger.debug("demand_planning: campaign weeks unavailable: %s", exc)
        return None


def _money(value: Optional[float], symbol: str) -> str:
    if value is None:
        return "n/a"
    return f"{symbol}{value:,.0f}"


def _range_money(lo: Optional[float], hi: Optional[float], symbol: str) -> str:
    if lo is None and hi is None:
        return "n/a"
    a, b = _money(lo, symbol), _money(hi, symbol)
    return a if a == b else f"{a}–{b}"


def _range_count(lo: Optional[float], hi: Optional[float]) -> str:
    if lo is None or hi is None:
        return "n/a"
    a, b = f"{int(round(lo)):,}", f"{int(round(hi)):,}"
    return a if a == b else f"{a}–{b}"


def build_goal_plan(
    *,
    goal: float,
    budget: float,
    projector: Projector,
    kind: str = GOAL_HIRES,
    weeks: Optional[float] = None,
    site_goals: Optional[Dict[str, int]] = None,
    sites: Optional[List[str]] = None,
    shares: Optional[List[float]] = None,
    symbol: str = "$",
    currency_code: str = "USD",
) -> Dict[str, Any]:
    """The goal plan for ``goal`` against the actual ``budget``.

    ``projector(b)`` is ``project_plan`` at budget ``b`` (all other inputs
    fixed).  ``weeks`` is the campaign length (None = unknown).  ``sites`` /
    ``shares`` (shares sum to 1) / ``site_goals`` add the per-site table.
    Returns a JSON-safe dict; ``ok`` False with ``error`` when the goal or the
    budget is unusable.  Never raises.
    """
    try:
        return _build_goal_plan(
            goal=goal,
            budget=budget,
            projector=projector,
            kind=kind,
            weeks=weeks,
            site_goals=site_goals or {},
            sites=sites or [],
            shares=shares or [],
            symbol=symbol,
            currency_code=currency_code,
        )
    except Exception as exc:  # noqa: BLE001 - a goal plan must never crash a caller
        logger.error("build_goal_plan failed: %s", exc, exc_info=True)
        return {"ok": False, "error": "Goal plan unavailable"}


def _build_goal_plan(
    *,
    goal: float,
    budget: float,
    projector: Projector,
    kind: str,
    weeks: Optional[float],
    site_goals: Dict[str, int],
    sites: List[str],
    shares: List[float],
    symbol: str,
    currency_code: str,
) -> Dict[str, Any]:
    goal = float(goal)
    budget = float(budget)
    if kind not in (GOAL_HIRES, GOAL_APPLY_STARTS) or goal <= 0 or goal > MAX_GOAL:
        return {"ok": False, "error": "A positive goal is required"}
    if budget <= 0:
        return {"ok": False, "error": "A positive budget is required"}
    unit = unit_label(kind)
    at_budget = projector(budget)
    metric = _metric(at_budget, kind)
    notes: List[str] = []
    base: Dict[str, Any] = {
        "ok": True,
        "kind": kind,
        "unit": unit,
        "goal": int(round(goal)),
        "budget": budget,
        "currency": {"code": currency_code, "symbol": symbol},
        "weeks": weeks,
        "notes": notes,
        "assumptions": {
            "basis": (
                "Inverted from the plan's own projection (the same engine the "
                "headline ranges use): required budget = the smallest budget at "
                "which the projected range reaches the goal."
            ),
            "client_hire_history": False,
        },
    }
    if metric is None:
        # declare, never convert: no local cost-per-hire benchmark -> a hires
        # goal cannot be priced; apply starts still can
        note = (
            "No local cost-per-hire benchmark for this market — a hires goal "
            "cannot be priced. Set an apply-start goal to plan backward."
            if kind == GOAL_HIRES
            else "The plan carries no apply-start projection."
        )
        notes.append(note)
        base.update(
            {
                "status": "not_projected",
                "required": {"low": None, "high": None, "status": "not_projected"},
                "projected": {"low": None, "high": None},
                "shortfall": {
                    "status": "not_projected",
                    "low": None,
                    "high": None,
                    "budget_gap_low": None,
                    "budget_gap_high": None,
                },
                "options": [],
                "sites": [],
                "text": {"callout": note, "headline": note},
            }
        )
        return base

    proj_lo, proj_hi = metric
    req = required_budget(goal, kind, projector, hint=budget)
    if not req["ok"]:
        notes.append(
            "No budget in the searchable range reaches this goal on the plan's "
            "projection."
            if req["status"] == "out_of_range"
            else "The goal cannot be priced from this plan's projection."
        )
        base.update(
            {
                "status": req["status"] or "unavailable",
                "required": {
                    "low": req.get("low"),
                    "high": req.get("high"),
                    "status": req["status"],
                },
                "projected": {"low": proj_lo, "high": proj_hi},
                "shortfall": {
                    "status": "unknown",
                    "low": None,
                    "high": None,
                    "budget_gap_low": None,
                    "budget_gap_high": None,
                },
                "options": [],
                "sites": [],
                "text": {"callout": notes[-1], "headline": notes[-1]},
            }
        )
        return base

    req_lo, req_hi = float(req["low"]), float(req["high"])
    short_lo = max(0.0, goal - proj_hi)  # optimistic end of the projection
    short_hi = max(0.0, goal - proj_lo)  # conservative end
    if proj_lo >= goal:
        status = "meets"
    elif proj_hi >= goal:
        status = "borderline"
    else:
        status = "short"
    gap_lo = max(0.0, req_lo - budget)
    gap_hi = max(0.0, req_hi - budget)

    # -- the three decision options -------------------------------------
    option_a = {
        "key": "A",
        "title": "Full objective",
        "summary": (
            f"Fund the goal: {_range_money(req_lo, req_hi, symbol)} delivers "
            f"{int(round(goal)):,} {unit}"
            + (
                f" ({_range_money(gap_lo, gap_hi, symbol)} more than the current budget)."
                if gap_hi > 0
                else " (the current budget already covers it)."
            )
        ),
        "budget_low": req_lo,
        "budget_high": req_hi,
        "delivers_low": goal,
        "delivers_high": goal,
        "shortfall_low": 0.0,
        "shortfall_high": 0.0,
        "extra_budget_low": gap_lo,
        "extra_budget_high": gap_hi,
    }
    option_b = {
        "key": "B",
        "title": "Current constraint",
        "summary": (
            f"Hold the budget at {_money(budget, symbol)}: it delivers about "
            f"{_range_count(proj_lo, proj_hi)} {unit} against {int(round(goal)):,}"
            + (
                f" — short by {_range_count(short_lo, short_hi)}."
                if short_hi > 0
                else " — the goal is covered."
            )
        ),
        "budget_low": budget,
        "budget_high": budget,
        "delivers_low": proj_lo,
        "delivers_high": proj_hi,
        "shortfall_low": short_lo,
        "shortfall_high": short_hi,
        "extra_budget_low": 0.0,
        "extra_budget_high": 0.0,
    }
    if weeks and weeks > 0:
        w_lo, w_hi = weeks * req_lo / budget, weeks * req_hi / budget
        option_c = {
            "key": "C",
            "title": "Extend the deadline",
            "kind": "extend_deadline",
            "weeks_now": weeks,
            "weeks_low": w_lo,
            "weeks_high": w_hi,
            "extra_weeks_low": max(0.0, w_lo - weeks),
            "extra_weeks_high": max(0.0, w_hi - weeks),
            "budget_low": req_lo,
            "budget_high": req_hi,
            "delivers_low": goal,
            "delivers_high": goal,
            "shortfall_low": short_lo,
            "shortfall_high": short_hi,
            "extra_budget_low": gap_lo,
            "extra_budget_high": gap_hi,
        }
        if gap_hi <= 0:
            option_c["summary"] = (
                "Not needed: the current budget covers the goal inside the "
                f"current {weeks:g}-week window."
            )
        else:
            ex_lo, ex_hi = option_c["extra_weeks_low"], option_c["extra_weeks_high"]
            ext = (
                f"{ex_lo:.1f}–{ex_hi:.1f}" if abs(ex_hi - ex_lo) >= 0.05 else f"{ex_hi:.1f}"
            )
            option_c["summary"] = (
                f"Keep the weekly spend ({_money(budget / weeks, symbol)}/week) and "
                f"extend the window by about {ext} weeks (to "
                f"{w_lo:.1f}–{w_hi:.1f} weeks): that reaches the goal. At the "
                f"original deadline the plan still lands "
                f"{_range_count(short_lo, short_hi)} {unit} short."
            )
    else:
        option_c = {
            "key": "C",
            "title": "Second flight",
            "kind": "second_flight",
            "weeks_now": None,
            "budget_low": req_lo,
            "budget_high": req_hi,
            "delivers_low": goal,
            "delivers_high": goal,
            "shortfall_low": short_lo,
            "shortfall_high": short_hi,
            "extra_budget_low": gap_lo,
            "extra_budget_high": gap_hi,
            "summary": (
                "Not needed: the current budget covers the goal."
                if gap_hi <= 0
                else (
                    f"Phase it: run the current budget first, then fund a second "
                    f"flight of {_range_money(gap_lo, gap_hi, symbol)} to close the "
                    f"{_range_count(short_lo, short_hi)} {unit} gap. (No campaign "
                    "length was stated, so no new deadline is computed.)"
                )
            ),
        }
    options = [option_a, option_b, option_c]

    # -- per-site table ---------------------------------------------------
    site_rows: List[Dict[str, Any]] = []
    if sites and shares and len(sites) == len(shares):
        for name, share in zip(sites, shares):
            g = goal_for_site(name, site_goals) if site_goals else None
            site_rows.append(
                {
                    "site": name,
                    "goal": g,
                    "share": share,
                    "budget": budget * share,
                    "projected_low": proj_lo * share,
                    "projected_high": proj_hi * share,
                    "gap_low": (max(0.0, g - proj_hi * share) if g is not None else None),
                    "gap_high": (max(0.0, g - proj_lo * share) if g is not None else None),
                }
            )

    # -- the one-liners every surface prints ------------------------------
    need = _range_money(req_lo, req_hi, symbol)
    cover = _range_count(proj_lo, proj_hi)
    callout = (
        f"Goal of {int(round(goal)):,} {unit} needs {need}; your budget of "
        f"{_money(budget, symbol)} covers ~{cover} {unit}."
    )
    if status == "meets":
        callout += " The goal is covered."
    elif status == "borderline":
        callout += " The goal is reachable at plan efficiency but not assured."
    else:
        callout += f" Short by {_range_count(short_lo, short_hi)} {unit}."
    base.update(
        {
            "status": status,
            "required": {"low": req_lo, "high": req_hi, "status": ""},
            "projected": {"low": proj_lo, "high": proj_hi},
            "shortfall": {
                "status": status,
                "low": short_lo,
                "high": short_hi,
                "budget_gap_low": gap_lo,
                "budget_gap_high": gap_hi,
            },
            "options": options,
            "sites": site_rows,
            "text": {
                "callout": callout,
                "headline": callout,
                "required": need,
                "covers": cover,
            },
        }
    )
    return base
