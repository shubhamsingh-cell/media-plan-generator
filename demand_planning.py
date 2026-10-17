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
import threading
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
_MAX_EXTENSION_WEEKS = 52  # a deadline extension past a year is not an option
_MAX_EXTENSION_FACTOR = 2.0  # ... nor one longer than twice the window
_GRID_POINTS = 120  # budget grid the non-monotone scan walks

Projector = Callable[[float], Dict[str, Any]]


# --------------------------------------------------------------------------
# quiet inversion: a search makes ~20 engine calls, each logging a dozen INFO
# lines.  A per-thread filter on the engine's logger mutes only THIS thread's
# search records (other requests keep logging).
# --------------------------------------------------------------------------
_quiet_state = threading.local()
_QUIET_LOGGERS = ("budget_engine",)


class _QuietFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        return not getattr(_quiet_state, "depth", 0)


_QUIET_FILTER = _QuietFilter()


class _quiet_engine:
    """Context manager muting the budget engine's log lines on this thread."""

    def __enter__(self) -> None:
        for name in _QUIET_LOGGERS:
            lg = logging.getLogger(name)
            if _QUIET_FILTER not in lg.filters:
                lg.addFilter(_QUIET_FILTER)
        _quiet_state.depth = getattr(_quiet_state, "depth", 0) + 1

    def __exit__(self, *exc: Any) -> None:
        _quiet_state.depth = max(0, getattr(_quiet_state, "depth", 1) - 1)


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


def unit_label(kind: str, singular: bool = False) -> str:
    if kind == GOAL_HIRES:
        return "hire" if singular else "hires"
    return "apply start" if singular else "apply starts"


def count_phrase(lo: Any, hi: Any, kind: str) -> str:
    """"1 hire", "300 hires", "24\u201348 hires": a count or range with its unit,
    singular only for exactly one."""
    a, b = int(round(float(lo))), int(round(float(hi)))
    rng = f"{a:,}" if a == b else f"{a:,}\u2013{b:,}"
    return f"{rng} {unit_label(kind, a == b == 1)}"


def city_key(text: Any) -> str:
    """Matching key for a site: the part before the first comma, alphanumerics
    only -- the same rule the weekly pacing split uses for location weights."""
    return re.sub(r"[^a-z0-9]", "", str(text or "").split(",")[0].lower())


# --------------------------------------------------------------------------
# D1: inverting project_plan
# --------------------------------------------------------------------------


def _bracket(
    value: Callable[[float], Optional[float]], goal: float, hint: float
) -> Tuple[Optional[float], str]:
    """A budget at which ``value`` reaches ``goal`` (not the smallest), found by
    proportional jumps from ``hint``: the counts are close to proportional to
    the budget, so ``goal / value`` lands near it in a step or two.
    ``(None, reason)`` when none up to the cap reaches it or the projection is
    unavailable."""
    hi = max(_START_BUDGET, hint)
    v = value(hi)
    if v is None:
        return None, "not_projected"
    steps = 0
    while v < goal and hi < _MAX_BUDGET and steps < _MAX_BRACKET_STEPS:
        factor = (goal / v * 1.02) if v > 0 else 4.0
        hi *= min(max(factor, 1.05), 1e4)
        nxt = value(hi)
        if nxt is None:
            return None, "not_projected"
        v = nxt
        steps += 1
    if v < goal:
        return None, "out_of_range"
    return hi, ""


def _refine(
    value: Callable[[float], Optional[float]], goal: float, lo: float, hi: float
) -> float:
    """Smallest budget in ``(lo, hi]`` (to ``_ABS_TOLERANCE`` dollars) at which
    ``value`` reaches ``goal``, given that it does at ``hi`` and does not at
    ``lo``."""
    for _ in range(_MAX_BISECT_STEPS):
        if hi - lo <= _ABS_TOLERANCE:
            break
        mid = (lo + hi) / 2.0
        r = value(mid)
        if r is not None and r >= goal:
            hi = mid
        else:
            lo = mid
    return float(math.ceil(hi))


def _scan(
    value: Callable[[float], Optional[float]],
    goal: float,
    points: List[float],
) -> Tuple[Optional[float], Optional[float], str]:
    """(safe, first, status) over the ascending budget grid ``points``.

    ``first`` is the smallest budget at which ``value`` reaches ``goal``.  The
    projections are NOT monotone in the budget -- an application count can step
    down when a channel floor or a benchmark band changes (a $50 larger budget
    projecting ~3% fewer applications) -- so ``safe`` is the smallest budget
    from which EVERY larger budget on the grid also reaches the goal: funding
    it never lands below the goal because of a dip above it.  ``safe == first``
    when the counts only rise.  ``(None, None, "out_of_range")`` when even the
    top of the grid falls short."""
    vals = [value(p) for p in points]
    if any(v is None for v in vals):
        return None, None, "not_projected"
    n = len(points)
    k = n
    while k > 0 and vals[k - 1] >= goal:  # type: ignore[operator]
        k -= 1
    if k == n:
        return None, None, "out_of_range"
    f = next(i for i, v in enumerate(vals) if v >= goal)  # type: ignore[operator]
    safe = _refine(value, goal, points[k - 1] if k > 0 else 0.0, points[k])
    first = safe if f == k else _refine(
        value, goal, points[f - 1] if f > 0 else 0.0, points[f]
    )
    return safe, min(first, safe), ""


def planning_step(budget: float) -> float:
    """The rounding increment a required budget is quoted in: whole dollars
    under $1,000, $10 under $10,000, else $100 (nobody plans a media budget
    to the dollar)."""
    return 1.0 if budget < 1000 else 10.0 if budget < 10000 else 100.0


def _quote(budget: float) -> float:
    """``budget`` rounded UP to its planning step (still reaches the goal: the
    counts never fall as the budget rises)."""
    step = planning_step(budget)
    return float(math.ceil(budget / step) * step)


def _settle(
    value: Callable[[float], Optional[float]], goal: float, budget: float
) -> float:
    """The smallest planning-step budget at or below ``_quote(budget)`` that
    still reaches ``goal`` (the search is exact to a dollar, so rounding up can
    land one step above the true minimum)."""
    q = _quote(budget)
    for _ in range(5):
        cand = q - planning_step(q)
        got = value(cand) if cand > 0 else None
        if got is not None and got >= goal:
            q = cand
        else:
            break
    return q


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
    it).  Quoted in planning steps ($10 under $10k, else $100), rounded UP, so
    ``projector(low)`` really reaches the goal and ``projector(low - step_low)``
    does not.
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

    def optimistic(b: float) -> Optional[float]:
        m = at(b)
        return None if m is None else m[1]

    def conservative(b: float) -> Optional[float]:
        m = at(b)
        return None if m is None else m[0]

    with _quiet_engine():
        top_lo, why = _bracket(optimistic, goal, hint)
        if top_lo is None:
            return {"ok": False, "status": why, "low": None, "high": None}
        top_hi, why = _bracket(conservative, goal, max(top_lo, hint))
        if top_hi is None:
            return {"ok": False, "status": why, "low": top_lo, "high": None}
        reach = max(top_lo, top_hi)
        span = reach * 1.3
        for _ in range(5):  # widen until the top of the grid reaches the goal
            points = [span * (i + 1) / _GRID_POINTS for i in range(_GRID_POINTS)]
            low_safe, low_first, why = _scan(optimistic, goal, points)
            high_safe, high_first, why2 = _scan(conservative, goal, points)
            if low_safe is not None and high_safe is not None:
                break
            span *= 1.6
        else:
            return {
                "ok": False,
                "status": why or why2 or "out_of_range",
                "low": None,
                "high": None,
            }
        low = _settle(optimistic, goal, low_safe)
        high = _settle(conservative, goal, max(low, high_safe))
    step_low, step_high = planning_step(low), planning_step(high)
    return {
        "ok": True,
        "status": "",
        "low": low,
        "high": high,
        "step_low": step_low,
        "step_high": step_high,
        # first budget that reaches the goal at all; below ``low`` / ``high``
        # only when the projection dips (see _scan)
        "first_reach_low": low_first,
        "first_reach_high": high_first,
        "non_monotone": bool(
            low_first < low - 2 * step_low or high_first < high - 2 * step_high
        ),
    }


# --------------------------------------------------------------------------
# D2: per-site goals
# --------------------------------------------------------------------------


_WHOLE_RE = re.compile(r"^\s*\d{1,9}(?:\.0+)?\s*$")


def whole_number(val: Any) -> Optional[int]:
    """``val`` as a whole number of goal units, or None.  Accepts an int (not a
    bool), an integral finite float, or plain digits ("250"); rejects exponent
    notation ("1e3"), signs, decimals, thousands separators and everything
    else -- a goal is never coerced from something it merely resembles."""
    if isinstance(val, bool) or val is None:
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val) if math.isfinite(val) and val == int(val) else None
    if isinstance(val, str) and _WHOLE_RE.match(val):
        return int(float(val.strip()))
    return None


def goal_problem(raw: Any, label: str = "The goal") -> Tuple[str, str]:
    """("", "") when ``raw`` is absent or a whole number from 1 to MAX_GOAL
    (0 / blank = no goal), else ("value", message)."""
    if raw in (None, ""):
        return "", ""
    n = whole_number(raw)
    if n is None:
        return "value", f"{label} must be a whole number."
    if n < 0:
        return "value", f"{label} cannot be negative."
    if n > MAX_GOAL:
        return "value", f"{label} exceeds the maximum ({MAX_GOAL:,})."
    return "", ""


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
        n = whole_number(val)
        if n is None:
            return "value", "Each site goal must be a whole number of hires."
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
        n = whole_number(val)
        if n is None:
            continue
        text = " ".join(re.sub(r"<[^>]+>", "", str(name)).split())
        if not text or n <= 0:
            continue
        key = text.lower()
        if key in seen:
            out[seen[key]] += n
        else:
            seen[key] = text
            out[text] = n
    return out


def _site_key(text: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(text or "").lower())


def _same_site(site: str, key: str) -> bool:
    """A goal key names a site when the two strings are the same place: equal
    ignoring case and punctuation, or either is a bare city ("Dallas") and the
    city parts agree.  "Springfield, IL" never matches "Springfield, MO"."""
    sk, kk = _site_key(site), _site_key(key)
    if sk and sk == kk:
        return True
    if ("," not in site or "," not in key) and city_key(site):
        return city_key(site) == city_key(key)
    return False


def assign_goals(
    sites: List[str], goals: Dict[str, int]
) -> Tuple[List[int], List[str]]:
    """(goal per site, goal keys that match no site).  Each goal key lands on
    exactly ONE site (an exact name beats a bare-city match); goals that land
    on the same site add up."""
    per = [0] * len(sites)
    unmatched: List[str] = []
    for key, val in (goals or {}).items():
        try:
            n = int(val)
        except (TypeError, ValueError):
            continue
        idx = next((i for i, s in enumerate(sites) if _site_key(s) == _site_key(key)), None)
        if idx is None:
            idx = next((i for i, s in enumerate(sites) if _same_site(s, key)), None)
        if idx is None:
            unmatched.append(key)
        else:
            per[idx] += n
    return per, unmatched


def goal_for_site(site: str, goals: Dict[str, int]) -> int:
    """The goal for ``site`` (0 = none)."""
    return assign_goals([site], goals)[0][0]


def unmatched_site_goals(goals: Dict[str, int], sites: List[str]) -> List[str]:
    """Goal names that match none of the plan's ``sites``."""
    return assign_goals(sites, goals)[1]


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
    per_site, _unmatched = assign_goals(sites, goals)
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
        with _quiet_engine():
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
            "cannot be priced. Enter an apply-start goal to plan backward."
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
    if req.get("non_monotone"):
        first = min(
            (req.get("first_reach_low") or req_lo), (req.get("first_reach_high") or req_hi)
        )
        notes.append(
            f"The projection is not smooth in this range: {_money(first, symbol)} "
            f"already reaches the goal, but a slightly larger budget projects "
            f"fewer {unit} (a channel floor or benchmark band changes). The "
            "required budget quoted is the lowest from which every larger "
            "budget also reaches it."
        )
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
            f"{count_phrase(goal, goal, kind)}"
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
            f"{count_phrase(proj_lo, proj_hi, kind)} against {int(round(goal)):,}"
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
        extra_hi = option_c["extra_weeks_high"]
        # a deadline extension is only a real alternative when it is a sane one:
        # past a year, or more than twice the window, the gap is a budget gap
        viable = w_hi <= _MAX_EXTENSION_WEEKS and extra_hi <= _MAX_EXTENSION_FACTOR * weeks
        fits_now = w_lo <= weeks  # the optimistic end already fits the window
        option_c["viable"] = viable
        option_c["fits_now"] = fits_now
        short_txt = (
            f"{count_phrase(short_lo, short_hi, kind)} short"
            if short_lo == short_hi
            else f"{_range_count(short_lo, short_hi)} {unit} short"
        )
        if gap_hi <= 0:
            option_c["summary"] = (
                "Not needed: the current budget covers the goal inside the "
                f"current {weeks:g}-week window."
            )
        elif not viable:
            option_c["title"] = "Extend the deadline (not viable alone)"
            option_c["summary"] = (
                "Extending the deadline alone can't close this gap: at the "
                f"current weekly spend ({_money(budget / weeks, symbol)}/week) the "
                f"goal would take about {w_hi:.0f} weeks against a {weeks:g}-week "
                "window. Add budget (option A) or lower the goal."
                + (
                    " At plan efficiency it already fits the current window."
                    if fits_now
                    else ""
                )
            )
        elif fits_now:
            option_c["summary"] = (
                f"At plan efficiency the goal already fits in the current "
                f"{weeks:g}-week window. At the industry-average cost, keep the "
                f"weekly spend ({_money(budget / weeks, symbol)}/week) and extend "
                f"to about {w_hi:.1f} weeks (+{extra_hi:.1f}); at the original "
                f"deadline the plan could land up to {short_txt}."
            )
        else:
            ex_lo = option_c["extra_weeks_low"]
            ext = (
                f"{ex_lo:.1f}\u2013{extra_hi:.1f}"
                if abs(extra_hi - ex_lo) >= 0.05
                else f"{extra_hi:.1f}"
            )
            option_c["summary"] = (
                f"Keep the weekly spend ({_money(budget / weeks, symbol)}/week) and "
                f"extend the window by about {ext} weeks (to "
                f"{w_lo:.1f}\u2013{w_hi:.1f} weeks): that reaches the goal. At the "
                f"original deadline the plan still lands {short_txt}."
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
                    f"{_range_count(short_lo, short_hi)}-{unit[:-1] if unit.endswith('s') else unit} gap. (No campaign "
                    "length was stated, so no new deadline is computed.)"
                )
            ),
        }
    options = [option_a, option_b, option_c]

    # -- per-site table ---------------------------------------------------
    site_rows: List[Dict[str, Any]] = []
    if sites and shares and len(sites) == len(shares):
        per_goal = assign_goals(sites, site_goals)[0] if site_goals else []
        for i, (name, share) in enumerate(zip(sites, shares)):
            # a site with no goal is "no goal" (None), never a goal of 0 that
            # would read as covered
            g = per_goal[i] if per_goal and per_goal[i] > 0 else None
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
        f"Goal of {count_phrase(goal, goal, kind)} needs {need}; your budget of "
        f"{_money(budget, symbol)} covers ~{count_phrase(proj_lo, proj_hi, kind)}."
    )
    if status == "meets":
        callout += " The goal is covered."
    elif status == "borderline":
        callout += " The goal is reachable at plan efficiency but not assured."
    else:
        callout += f" Short by {count_phrase(short_lo, short_hi, kind)}."
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
