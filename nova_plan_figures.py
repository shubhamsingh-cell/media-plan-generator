#!/usr/bin/env python3
"""One source of plan-level numbers for Nova chat.

Investigation 2026-10-18 (a 60k, 3-month brief read 900 applications / 14 hires
/ $66.67 CPA in chat while the plan generator said 2,400-7,500 / 17-35 /
$8-$25): Nova had SEVERAL calculators that each priced the same budget from their
own private tables, and nothing at the answer boundary checked a hire or
application count. ``plan_projection.project_plan`` is the ONE engine (the
wizard's /api/estimate, the deliverables' headline and Quick Plan already read
it); this module is what makes Nova read it too:

* :func:`resolve_budget` -- the campaign total a budget means, read with the
  wizard's own rule (``wizard_inputs.resolve_plan_budget``), with a sentence
  stating the reading ("planning on 60,000 as the TOTAL campaign budget").
* :func:`plan_headline` -- the engine's projection for a brief, for tools that
  are not the projection tool.
* ``withhold_*`` -- remove the plan totals a second calculator computed from its
  own tables (it keeps its channel-level rows: WHERE to spend is its job).
* :func:`small_budget_guidance` -- an honest, useful reading of a budget that
  funds under a few hires.
* :func:`enforce` -- the answer-boundary guard: a hires / applications / CPA /
  cost-per-hire figure the model typed that is not the engine's is rewritten to
  the engine's range (prose) or marked (tables), and a plan answer always
  carries the engine's ranges with their basis and which end is the best case.

Pure functions, no Nova import, never raises into a caller.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

logger = logging.getLogger(__name__)

DAGGER = "†"
CORRECTED_LINE = "*Figures above were corrected to Nova's planning engine projection for this brief.*"
BEST_CASE_SENTENCE = (
    "The high end of applications and hires, and the low end of cost per "
    "application and cost per hire, are the best case (the plan-efficiency floor); "
    "the other end is the industry average."
)
RANGE_PLAN_NOTE = (
    "plan_projection is the ONE set of plan-level applications, hires, cost per "
    "application and cost per hire for this budget (the same engine the plan "
    "generator uses). Quote those ranges, never a single number, and never "
    "compute, average, rescale or back-calculate them yourself. Channel rows in "
    "this result are this tool's own channel-level reference, not a plan total; "
    "do not add them up into one."
)

# kinds of plan-level figure
HIRES, APPS, CPA, CPH = "hires", "applications", "cpa", "cph"
KINDS = (HIRES, APPS, CPA, CPH)

# ---------------------------------------------------------------------------
# Budget reading
# ---------------------------------------------------------------------------
_PERIOD_ALIASES = {
    "": "campaign", "campaign": "campaign", "total": "campaign", "overall": "campaign",
    "one-time": "campaign", "lump sum": "campaign", "month": "monthly",
    "monthly": "monthly", "per month": "monthly", "quarter": "quarterly",
    "quarterly": "quarterly", "per quarter": "quarterly", "year": "annual",
    "yearly": "annual", "annual": "annual", "annually": "annual", "per year": "annual",
}  # fmt: skip
_PERIOD_UNIT = {"monthly": "month", "quarterly": "quarter", "annual": "year"}


def normalize_period(value: Any) -> str:
    """``campaign`` | ``monthly`` | ``quarterly`` | ``annual`` (unknown -> campaign)."""
    return _PERIOD_ALIASES.get(str(value or "").strip().lower(), "campaign")


def _duration_label(months: Any) -> str:
    if months in (None, ""):
        return ""
    if isinstance(months, (int, float)) and not isinstance(months, bool):
        return f"{months:g} months" if months > 0 else ""
    return str(months).strip()


def _money(value: float) -> str:
    return f"${value:,.0f}"


def resolve_budget(raw: Any, period: Any = None, months: Any = None) -> Dict[str, Any]:
    """The campaign total ``raw`` means, read exactly as the wizard reads a
    budget (amount x periods over the campaign's months, bounds checked).

    Returns ``{"ok", "total", "error", "reading"}``; ``reading`` is the object a
    tool result carries so the model can say how the budget was read.
    """
    try:
        import wizard_inputs

        per = normalize_period(period)
        label = _duration_label(months)
        plan = wizard_inputs.resolve_plan_budget(raw, per, label or None, None)
    except Exception as exc:  # noqa: BLE001 - a bad budget must not crash a chat turn
        logger.warning("nova_plan_figures: budget reading failed: %s", exc)
        return {
            "ok": False,
            "total": 0.0,
            "error": "Budget could not be read",
            "reading": {},
        }
    if not plan.ok:
        return {
            "ok": False,
            "total": 0.0,
            "error": plan.message or "Budget could not be read",
            "reading": {},
        }
    total = float(plan.total)
    if per == "campaign":
        tail = f" over {plan.months:g} months" if label else ""
        text = (
            f"Planning on {_money(total)} as the TOTAL campaign budget{tail}. If that "
            "figure is per month, per quarter or per year, say so and I will re-run it."
        )
        assumed = False
    else:
        unit = _PERIOD_UNIT.get(per, per)
        assumed = not label
        length = f"{plan.months:g} months" + (
            " (assumed: the planner's default campaign length; tell me the real one)"
            if assumed
            else ""
        )
        text = (
            f"Planning on {_money(total)}: {_money(float(plan.parse.amount))} per {unit} "
            f"over {length}."
        )
    return {
        "ok": True,
        "total": total,
        "error": "",
        "reading": {
            "input_amount": float(plan.parse.amount),
            "period": per,
            "months": float(plan.months),
            "months_assumed": bool(assumed),
            "campaign_total": total,
            "text": text,
        },
    }


# ---------------------------------------------------------------------------
# The engine's headline for a brief (tools that are not the projection tool)
# ---------------------------------------------------------------------------
def plan_headline(
    budget: float,
    roles: Optional[Sequence[Any]] = None,
    locations: Optional[Sequence[Any]] = None,
    industry: Any = "",
) -> Optional[Dict[str, Any]]:
    """``plan_projection.public_view`` of the engine's projection for the brief,
    or None when the engine cannot price it. Same inputs as /api/estimate."""
    try:
        import plan_projection

        titles = [str(r).strip() for r in (roles or []) if str(r or "").strip()]
        locs = [str(x).strip() for x in (locations or []) if str(x or "").strip()]
        proj = plan_projection.project_plan(
            {
                "budget": float(budget),
                "roles": titles or ["General Hire"],
                "locations": locs or ["United States"],
                "industry": str(industry or ""),
                "budget_text": f"${float(budget):,.0f}",
            },
            plan_currency="USD",
        )
        if not proj.get("ok"):
            return None
        return plan_projection.public_view(proj)
    except Exception as exc:  # noqa: BLE001
        logger.warning("nova_plan_figures: plan headline unavailable: %s", exc)
        return None


def attach_plan_headline(
    result: Dict[str, Any],
    budget: Any,
    roles: Optional[Sequence[Any]],
    locations: Optional[Sequence[Any]],
    industry: Any,
) -> Dict[str, Any]:
    """Stamp the engine's projection on a second calculator's result."""
    try:
        total = float(budget or 0)
    except (TypeError, ValueError):
        total = 0.0
    if total <= 0 or not isinstance(result, dict):
        return result
    head = plan_headline(total, roles, locations, industry)
    result["plan_projection_note"] = RANGE_PLAN_NOTE
    if head:
        result["plan_projection"] = head
        result["plan_projection_budget"] = total
    else:
        result["plan_projection_note"] = (
            "This tool does not state plan-level applications, hires or cost per hire "
            "for a budget. Call query_budget_projection for them; do not estimate them."
        )
    return result


# ---------------------------------------------------------------------------
# Withholding a second calculator's plan totals
# ---------------------------------------------------------------------------
_RECOMMENDER_EXPECTED_RE = re.compile(r"\s*Expected:[^\n]*?avg CPA\.", re.IGNORECASE)
_RECOMMENDER_APPS_RE = re.compile(r"\s*\|\s*Apps:\s*~[\d,]+")
_HIRES_SENTENCE_RE = re.compile(
    r"[^.\n]*\b(?:hires?|CPA|CPH|cost per hire)\b[^.\n]*\d[^.\n]*\.?", re.IGNORECASE
)


def withhold_recommender_totals(rec: Dict[str, Any], formatter: Any = None) -> None:
    """channel_recommender's per-channel outcome projections and its plan totals
    (917 applications / 17 hires for a 60k budget) came from a private
    table that disagreed with the plan generator. Keep WHERE to spend (share,
    spend, CPC, CPA benchmark, rationale); drop the outcomes."""
    if not isinstance(rec, dict):
        return
    if isinstance(rec.get("summary"), str):
        rec["summary"] = _RECOMMENDER_EXPECTED_RE.sub("", rec["summary"]).strip()
    meta = rec.get("metadata")
    if isinstance(meta, dict):
        for key in (
            "total_projected_clicks", "total_projected_applications",
            "total_projected_hires", "avg_cpa",
        ):  # fmt: skip
            meta.pop(key, None)
    if formatter is not None:
        try:
            text = formatter(rec)
            rec["formatted_text"] = _RECOMMENDER_APPS_RE.sub("", text)
        except Exception as exc:  # noqa: BLE001
            logger.debug("nova_plan_figures: recommender text unavailable: %s", exc)
    for tier in ("must_have", "should_have", "test_and_learn", "skip"):
        for ch in rec.get(tier) or []:
            if isinstance(ch, dict):
                for key in (
                    "projected_applications",
                    "projected_hires",
                    "projected_clicks",
                ):
                    ch.pop(key, None)


def withhold_optimizer_totals(res: Dict[str, Any]) -> None:
    """campaign_optimizer's totals / scenarios (96 hires for a 60k budget)
    and per-channel outcome projections."""
    if not isinstance(res, dict):
        return
    res.pop("total_projected", None)
    res.pop("scenarios", None)
    for ch in (res.get("recommended_allocation") or {}).values():
        if isinstance(ch, dict):
            for key in (
                "projected_clicks",
                "projected_applies",
                "projected_hires",
                "cph",
            ):
                ch.pop(key, None)
    for key in ("recommendations", "warnings"):
        if isinstance(res.get(key), list):
            res[key] = [
                x
                for x in res[key]
                if not (
                    isinstance(x, str)
                    and re.search(r"\b(?:hires?|cph|cost per hire)\b", x, re.I)
                    and re.search(r"\d", x)
                )
            ]


def withhold_prediction_totals(res: Dict[str, Any]) -> None:
    """prediction_model's applications / qualified / cost per hire (it printed
    the whole budget as the cost per hire of the plan)."""
    if not isinstance(res, dict):
        return
    pred = res.get("prediction")
    if isinstance(pred, dict):
        for key in (
            "predicted_applications",
            "predicted_qualified",
            "predicted_cost_per_hire",
        ):
            pred.pop(key, None)


# ---------------------------------------------------------------------------
# Small budgets
# ---------------------------------------------------------------------------
_CHANNEL_LABELS = {
    "programmatic_dsp": "Programmatic & DSP",
    "global_boards": "Global job boards",
    "niche_boards": "Niche / industry boards",
    "social_media": "Social media",
    "regional_boards": "Regional & local boards",
    "employer_branding": "Employer branding",
    "apac_regional": "APAC regional boards",
    "emea_regional": "EMEA regional boards",
}  # fmt: skip

SMALL_BUDGET_MAX_HIRES = 3
SMALL_BUDGET_MAX_BUDGET = 5_000.0


def _num(v: Any) -> Optional[float]:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return float(v)


def small_budget_guidance(
    proj: Dict[str, Any], budget: float, channel_allocations: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """What a small budget can and cannot do, from the engine's own figures.

    Applies when the budget funds fewer than three hires even at the plan
    efficiency floor, or is at most $5,000. There is no minimum budget: the
    answer is the application range, the 1-3 channels with the lowest modelled
    cost per application, and the budget one hire needs on the engine's basis.
    """
    try:
        hires = proj.get("hires") or {}
        text = proj.get("text") or {}
        a = proj.get("assumptions") or {}
        h_hi = hires.get("high")
        h_lo = hires.get("low")
        small = (isinstance(h_hi, (int, float)) and h_hi < SMALL_BUDGET_MAX_HIRES) or (
            budget <= SMALL_BUDGET_MAX_BUDGET
        )
        apps_text = str(text.get("applications") or "")
        if not small or not apps_text:
            return None
        floor, avg = _num(a.get("industry_cph_floor")), _num(a.get("industry_cph_avg"))
        suppressed = bool(a.get("hires_suppressed"))
        focus: List[Dict[str, Any]] = []
        ranked = []
        for key, ch in (channel_allocations or {}).items():
            if not isinstance(ch, dict) or key == "employer_branding":
                continue
            if ch.get("channel_role") == "brand":
                continue
            cpa, spend = _num(ch.get("cpa")), _num(ch.get("dollar_amount"))
            if cpa and cpa > 0 and spend and spend > 0:
                ranked.append((cpa, key, ch))
        for cpa, key, ch in sorted(ranked)[:3]:
            focus.append(
                {
                    "channel": _CHANNEL_LABELS.get(key, key.replace("_", " ").title()),
                    "key": key,
                    "modelled_cost_per_application": round(cpa, 2),
                }
            )
        if suppressed:
            hires_part = "Hires are not projected for this market."
        elif isinstance(h_hi, (int, float)) and h_hi < 1:
            hires_part = (
                f"That is fewer than one hire at this budget ({text.get('hires') or '0'} "
                "hires, an estimate), so treat applications, not hires, as the measure."
            )
        elif isinstance(h_lo, (int, float)) and h_lo < 1:
            hires_part = (
                f"That is fewer than one hire at the low end ({text.get('hires') or '0'} "
                "hires, an estimate), so treat applications, not hires, as the measure."
            )
        else:
            hires_part = f"Hires: {text.get('hires') or 'n/a'} (an estimate)."
        out: Dict[str, Any] = {
            "text": (
                f"At {_money(budget)} the plan buys roughly {apps_text} applications. "
                f"{hires_part}"
            ),
            "focus_channels": focus,
            "what_it_can_do": (
                "Put a role in front of candidates and collect applications from one to "
                "three channels."
            ),
            "what_it_cannot_do": (
                "Spread across six channels, fund employer branding, or promise a "
                "particular number of hires."
            ),
            "instruction": (
                "There is no minimum budget: do not refuse and do not invent one. Give "
                "the application range, recommend putting the budget into focus_channels "
                "(the lowest modelled cost per application) rather than all six, say "
                "what the budget can and cannot do, and state what one hire would take "
                "(one_hire_budget) using only these engine figures."
            ),
        }
        if floor and avg:
            out["one_hire_budget"] = {"low": round(floor, 2), "high": round(avg, 2)}
            out["one_hire_budget_text"] = (
                f"One hire takes about {_money(floor)}-{_money(avg)} on the engine's "
                "cost-per-hire basis (best case to industry average)."
            )
        return out
    except Exception as exc:  # noqa: BLE001
        logger.warning("nova_plan_figures: small-budget guidance failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Engine figures (what a tool result says the plan figures ARE)
# ---------------------------------------------------------------------------
class EngineFigures:
    """Plan-level figures of one projection: the printable range per kind, the
    set of plan-level numbers (``plan``), the per-channel numbers (``channel``)
    and the labels."""

    def __init__(self) -> None:
        self.text: Dict[str, str] = {}
        self.range: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
        self.plan: Dict[str, Set[float]] = {k: set() for k in KINDS}
        self.channel: Dict[str, Set[float]] = {k: set() for k in KINDS}
        self.other: Dict[str, Set[float]] = {k: set() for k in KINDS}
        self.labels: Dict[str, str] = {}
        self.budget: Optional[float] = None
        self.reading: str = ""
        self.suppressed: Set[str] = set()
        self.signature: Tuple[Any, ...] = ()
        # True for the projection tool's own result; False for the engine headline
        # another tool carries (built from whatever brief the model passed that
        # tool, so it can differ from the projection tool's for the same plan)
        self.primary: bool = False
        # True when the budget was SOLVED from a hiring goal (no budget given):
        # the figures are for that solved budget, so no block is appended
        self.solved: bool = False

    def rewritable(self, kind: str) -> bool:
        return bool(self.text.get(kind)) and kind not in self.suppressed

    def nondegenerate(self, kind: str) -> bool:
        lo, hi = self.range.get(kind, (None, None))
        return lo is not None and hi is not None and lo != hi


def _kind_for_key(key: str) -> Optional[str]:
    k = key.lower()
    if "cost_per_application" in k or re.search(r"(?:^|_)cpa(?:$|_)", k):
        return CPA
    if "cost_per_hire" in k or re.search(r"(?:^|_)cph(?:$|_)", k):
        return CPH
    if "hires" in k or k == "hire":
        return HIRES
    if k in ("applications", "applies") or "applications" in k or "applies" in k:
        return APPS
    return None


def _numbers_in(value: Any) -> List[float]:
    out: List[float] = []
    if isinstance(value, bool):
        return out
    if isinstance(value, (int, float)):
        out.append(float(value))
    elif isinstance(value, str):
        for tok in re.findall(r"\d[\d,]*(?:\.\d+)?", value):
            try:
                out.append(float(tok.replace(",", "")))
            except ValueError:
                pass
    return out


def _walk_kind_numbers(
    obj: Any, into: Dict[str, Set[float]], kind: Optional[str] = None, depth: int = 0
) -> None:
    """Every number under a key that names a kind (``cpa``, ``projected_hires``,
    ``cost_per_hire`` ...), including range strings like "$35-$85"."""
    if depth > 9:
        return
    if isinstance(obj, dict):
        for key, val in obj.items():
            k = _kind_for_key(str(key)) or kind
            if isinstance(val, (dict, list, tuple)):
                _walk_kind_numbers(val, into, k, depth + 1)
            elif k:
                for n in _numbers_in(val):
                    into[k].add(n)
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            _walk_kind_numbers(item, into, kind, depth + 1)
    elif kind:
        for n in _numbers_in(obj):
            into[kind].add(n)


def _range_of(block: Any) -> Tuple[Optional[float], Optional[float]]:
    if isinstance(block, dict):
        return _num(block.get("low")), _num(block.get("high"))
    return None, None


def figures_from_projection(
    proj: Dict[str, Any],
    channel_allocations: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
    budget: Optional[float] = None,
    reading: str = "",
) -> Optional[EngineFigures]:
    """EngineFigures for one ``public_view``-shaped projection (or None)."""
    if not isinstance(proj, dict) or not isinstance(proj.get("text"), dict):
        return None
    fig = EngineFigures()
    text = proj["text"]
    a = proj.get("assumptions") if isinstance(proj.get("assumptions"), dict) else {}
    eng = proj.get("engine") if isinstance(proj.get("engine"), dict) else {}
    fig.budget = budget
    fig.reading = reading
    if a.get("hires_suppressed"):
        fig.suppressed.update({HIRES, CPH})
    for kind in KINDS:
        txt = str(text.get(kind) or "").strip()
        if txt:
            fig.text[kind] = txt
        fig.range[kind] = _range_of(proj.get(kind))
        block = proj.get(kind) if isinstance(proj.get(kind), dict) else {}
        for key in ("low", "mid", "high"):
            n = _num(block.get(key))
            if n is not None:
                fig.plan[kind].add(n)
    for src_key, kind in (
        ("applications", APPS), ("hires", HIRES),
        ("cost_per_application", CPA), ("cost_per_hire", CPH),
    ):  # fmt: skip
        n = _num(eng.get(src_key))
        if n is not None:
            fig.plan[kind].add(n)
    for key in ("industry_cph_avg", "industry_cph_floor"):
        n = _num(a.get(key))
        if n is not None:
            fig.plan[CPH].add(n)
    for key in (
        "hires_label",
        "cph_label",
        "hires_short",
        "cph_short",
        "hires_basis",
        "cph_basis",
        "hires_note",
    ):
        if text.get(key):
            fig.labels[key] = str(text[key])
    if isinstance(channel_allocations, dict):
        _walk_kind_numbers(list(channel_allocations.values()), fig.channel)
    if isinstance(extra, dict):
        _walk_kind_numbers(extra, fig.other)
    for kind in KINDS:
        # the engine's point figures also sit in sufficiency / target comparisons:
        # they are the best-case END of a range, not another answer
        fig.other[kind] -= fig.plan[kind]
    fig.signature = tuple(fig.text.get(k, "") for k in KINDS)
    return fig


def _parse_json_blob(raw: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return None
    start = raw.find("{")
    if start < 0:
        return None
    try:
        obj, _ = json.JSONDecoder().raw_decode(raw[start:])
    except ValueError:
        return None
    return obj if isinstance(obj, dict) else None


def _figures_from_result(res: Dict[str, Any]) -> Optional[EngineFigures]:
    proj = None
    primary = False
    for key in ("projection", "plan_projection", "headline_projection"):
        if isinstance(res.get(key), dict) and isinstance(res[key].get("text"), dict):
            proj = res[key]
            primary = key == "projection"
            break
    if proj is None:
        return None
    reading = ""
    br = res.get("budget_reading")
    if isinstance(br, dict):
        reading = str(br.get("text") or "")
    budget = _num(res.get("total_budget")) or _num(res.get("plan_projection_budget"))
    # the engine's point figures (total_projected) are the optimistic END of each
    # range, not a second answer: they stay out of the "other tool numbers" pool
    skip = (
        "channel_allocations",
        "projection",
        "plan_projection",
        "headline_projection",
        "total_projected",
    )
    extra = {k: v for k, v in res.items() if k not in skip}
    fig = figures_from_projection(
        proj,
        res.get("channel_allocations"),
        extra=extra,
        budget=budget,
        reading=reading,
    )
    if fig is not None:
        fig.primary = primary
        fig.solved = res.get("budget_source") == "solved_for_goal"
    return fig


def collect(
    tool_results: Iterable[Any],
) -> Tuple[List[EngineFigures], List[Dict[str, Any]]]:
    """(distinct engine projections, every parsed non-projection tool result)."""
    engines: List[EngineFigures] = []
    others: List[Dict[str, Any]] = []
    seen: Set[Tuple[Any, ...]] = set()
    for raw in tool_results or []:
        res = _parse_json_blob(raw)
        if res is None:
            continue
        fig = _figures_from_result(res)
        if fig is None:
            others.append(res)
            continue
        if fig.signature in seen:
            continue
        seen.add(fig.signature)
        engines.append(fig)
    return engines, others


# ---------------------------------------------------------------------------
# Claims in the model's answer
# ---------------------------------------------------------------------------
_NUM = r"(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?"
_COUNT = rf"~?{_NUM}(?:\s?[kK]\b)?"
_MONEY = rf"~?\$\s?{_NUM}(?:\s?[kKmM]\b)?"
_MONEY_NEXT = rf"~?\$?\s?{_NUM}(?:\s?[kKmM]\b)?"
_SEP = r"\s*(?:–|—|-|to)\s*"
_VAL_COUNT = rf"(?P<val>{_COUNT}(?:{_SEP}{_COUNT})?)"
_VAL_MONEY = rf"(?P<val>{_MONEY}(?:{_SEP}{_MONEY_NEXT})?)"
_ADJ = r"(?:(?:projected|estimated|expected|total|new|additional|qualified|full[- ]time|net)\s+)*"
_LEAD = r"(?:of|is|are|at|:|=|around|about|approximately|roughly|averag\w*|totall?ing|ranges?\s+from|between|~)?\s*"
_NB = r"(?<![\d$.,/])"

_CLAIM_RES: Dict[str, List["re.Pattern[str]"]] = {
    HIRES: [
        re.compile(rf"{_NB}{_VAL_COUNT}\s*\+?\s*{_ADJ}(?:hires?|placements?)\b", re.I),
        re.compile(rf"\bhires?\s*(?:of|:|=|are|is|at|about|around|approximately|roughly|totall?ing)\s*{_VAL_COUNT}(?!\s*(?:%|per\b|/))", re.I),
    ],
    APPS: [
        re.compile(rf"{_NB}{_VAL_COUNT}\s*{_ADJ}(?:applications?|applies|apps|apply[- ]starts?)\b", re.I),
        re.compile(rf"\b(?:applications?|applies|apps)\s*(?:of|:|=|are|is|at|about|around|approximately|roughly|totall?ing)\s*{_VAL_COUNT}(?!\s*(?:%|per\b|/))", re.I),
    ],
    CPA: [
        re.compile(rf"\bCPA\b\s*(?:\([^)\n]{{0,30}}\)\s*)?{_LEAD}{_VAL_MONEY}", re.I),
        re.compile(rf"{_VAL_MONEY}\s*(?:(?:avg\.?|average|blended)\s+)?(?:CPA\b|per\s+(?:application|apply|applicant)\b|cost[- ]per[- ]application\b)", re.I),
        re.compile(rf"\bcost[- ]per[- ]application\b\s*(?:\(CPA\)\s*)?{_LEAD}{_VAL_MONEY}", re.I),
    ],
    CPH: [
        re.compile(rf"\bCPH\b\s*(?:\([^)\n]{{0,30}}\)\s*)?{_LEAD}{_VAL_MONEY}", re.I),
        re.compile(rf"{_VAL_MONEY}\s*(?:(?:avg\.?|average|blended)\s+)?(?:CPH\b|per\s+hire\b|/\s?hire\b|cost[- ]per[- ]hire\b)", re.I),
        re.compile(rf"\bcost[- ]per[- ]hire\b\s*(?:\(CPH\)\s*)?{_LEAD}{_VAL_MONEY}", re.I),
    ],
}  # fmt: skip
_TOKEN_RE = re.compile(rf"~?\$?\s?(?P<n>{_NUM})(?:\s?(?P<s>[kKmM])\b)?")


def _endpoints(val: str) -> List[Tuple[float, int, float]]:
    """[(value, decimals, scale)] for "17-35", "$8-$25", "2.5k-4k"."""
    out: List[Tuple[float, int, float]] = []
    for m in _TOKEN_RE.finditer(val):
        raw = m.group("n").replace(",", "")
        try:
            value = float(raw)
        except ValueError:
            continue
        decimals = len(raw.split(".")[1]) if "." in raw else 0
        suffix = (m.group("s") or "").lower()
        scale = 1000.0 if suffix == "k" else 1e6 if suffix == "m" else 1.0
        out.append((value, decimals, scale))
    return out


def _matches(value: float, decimals: int, scale: float, pool: Iterable[float]) -> bool:
    for a in pool:
        if abs(a / scale - value) <= 0.5 * 10 ** (-decimals) + 1e-9:
            return True
        if a and abs(a - value * scale) / abs(a) <= 0.005:
            return True
    return False


def _all_in(ends: List[Tuple[float, int, float]], *pools: Iterable[float]) -> bool:
    merged: List[float] = []
    for p in pools:
        merged.extend(p)
    return bool(ends) and all(_matches(v, d, s, merged) for v, d, s in ends)


def user_number_pool(message: str) -> str:
    """The numbers the user typed, as a tool-result-shaped JSON string, so the
    generic number verifier treats an echoed budget as traced."""
    return json.dumps({"user_numbers": sorted(_user_numbers(message))})


def _user_numbers(message: str) -> Set[float]:
    out: Set[float] = set()
    for m in _TOKEN_RE.finditer(message or ""):
        try:
            v = float(m.group("n").replace(",", ""))
        except ValueError:
            continue
        suffix = (m.group("s") or "").lower()
        out.add(v * (1000.0 if suffix == "k" else 1e6 if suffix == "m" else 1.0))
    return out


_CHANNEL_CUE_RE = re.compile(
    r"\b(?:programmatic|dsp|indeed|linkedin|ziprecruiter|zip\s?recruiter|glassdoor|google|bing|"
    r"meta|facebook|instagram|tiktok|snapchat|social|niche|regional|global|job\s+boards?|boards?|"
    r"channels?|employer\s+branding|craigslist|monster|careerbuilder|per\s+channel|each|alone|only)\b",
    re.IGNORECASE,
)


def _sentence_around(text: str, start: int, end: int) -> str:
    left = max(
        text.rfind(".", 0, start), text.rfind("!", 0, start),
        text.rfind("?", 0, start), text.rfind("\n", 0, start),
    )  # fmt: skip
    cut = [
        i
        for i in (
            text.find(".", end),
            text.find("!", end),
            text.find("?", end),
            text.find("\n", end),
        )
        if i >= 0
    ]
    return text[left + 1 : min(cut) if cut else len(text)]


def _has_channel_cue(text: str, start: int, end: int) -> bool:
    """True when the sentence around a claim talks about ONE channel or board
    ("Programmatic alone is modelled at 12 hires"): only then may the claim be
    one of the per-channel figures. A plan-level sentence never borrows a
    channel's small integer (a channel's 14 hires must not excuse "14 hires" for
    the whole plan)."""
    return bool(_CHANNEL_CUE_RE.search(_sentence_around(text, start, end)))


_BENCH_CUE_RE = re.compile(
    r"\b(?:benchmarks?|typical(?:ly)?|norms?|median|shrm|appcast|historically|generally|usually|"
    r"industry[- ](?:average|range|standard|rate)|national average|market (?:rate|average)|"
    r"gross cpa|net cpa)\b",
    re.IGNORECASE,
)


def _has_benchmark_cue(text: str, start: int, end: int) -> bool:
    """A sentence quoting an industry benchmark ("typical nurse CPA is $35-$85")
    is not a statement about THIS plan: when its figure matches nothing it is
    marked, never rewritten to the plan's range."""
    return bool(_BENCH_CUE_RE.search(_sentence_around(text, start, end)))


_USER_GOAL_RE = re.compile(
    rf"\b(?:hire|hiring|hires|fill|recruit(?:ing)?|need|needs|want|wants|target|goal\s+of|get|reach)\s+"
    rf"(?:about\s+|around\s+|up\s+to\s+|at\s+least\s+)?(?P<n>{_NUM})\b(?!\s*%)",
    re.IGNORECASE,
)


def _user_pools(message: str) -> Dict[str, Set[float]]:
    """The figures the USER stated, by kind, so an echo ("your goal of 40 hires",
    "under $20 per application") is never read as an invention. Counts count
    only beside a hire / application word or a hire verb: a "3" in "3-month"
    must not excuse "3 hires"; dollar amounts are accepted as typed."""
    pools: Dict[str, Set[float]] = {k: set() for k in KINDS}
    msg = message or ""
    for start, end, kind in _claims(msg):
        for value, _d, scale in _endpoints(msg[start:end]):
            pools[kind].add(value * scale)
    for m in _USER_GOAL_RE.finditer(msg):
        try:
            pools[HIRES].add(float(m.group("n").replace(",", "")))
        except ValueError:
            pass
    dollars = {
        value * scale
        for tok in re.findall(_MONEY, msg)
        for value, _d, scale in _endpoints(tok)
    }
    pools[CPA] |= dollars
    pools[CPH] |= dollars
    return pools


def _claims(text: str) -> List[Tuple[int, int, str]]:
    """[(start, end, kind)] of the ``val`` span of every plan-figure claim,
    non-overlapping, in order."""
    found: List[Tuple[int, int, str]] = []
    for kind, patterns in _CLAIM_RES.items():
        for rx in patterns:
            for m in rx.finditer(text):
                found.append((m.start("val"), m.end("val"), kind))
    found.sort(key=lambda c: (c[0], -(c[1] - c[0])))
    out: List[Tuple[int, int, str]] = []
    last_end = -1
    for start, end, kind in found:
        if start < last_end:
            continue
        out.append((start, end, kind))
        last_end = end
    return out


_TABLE_KIND_RES = (
    (CPH, re.compile(r"\bcph\b|cost[ /]*(?:per[ /]*)?hire", re.I)),
    (CPA, re.compile(r"\bcpa\b|cost[ /]*(?:per[ /]*)?app", re.I)),
    (APPS, re.compile(r"\bapplications?\b|\bapplies\b|\bapps\b", re.I)),
    (HIRES, re.compile(r"\bhires?\b", re.I)),
)
_SEP_ROW_RE = re.compile(r"^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)*\|?\s*$")


def _table_kind(header: str) -> Optional[str]:
    for kind, rx in _TABLE_KIND_RES:
        if rx.search(header):
            return kind
    return None


def _mark_table_cells(
    text: str,
    fig: Optional[EngineFigures],
    user: Dict[str, Set[float]],
    others: Dict[str, Set[float]],
) -> Tuple[str, int]:
    """Dagger every number in a Hires / CPA / CPH / Applications column that is
    not one of the engine's (plan or channel), the user's, or another tool's
    benchmark. Returns (text, marks)."""
    lines = text.split("\n")
    marks = 0
    i = 0
    while i < len(lines) - 1:
        if not lines[i].lstrip().startswith("|") or not _SEP_ROW_RE.match(lines[i + 1]):
            i += 1
            continue
        headers = [c.strip() for c in lines[i].strip().strip("|").split("|")]
        kinds = [_table_kind(h) for h in headers]
        j = i + 2
        while j < len(lines) and lines[j].lstrip().startswith("|"):
            cells = lines[j].strip().strip("|").split("|")
            rebuilt = []
            for idx, cell in enumerate(cells):
                kind = kinds[idx] if idx < len(kinds) else None
                if kind and DAGGER not in cell:
                    cell, n = _mark_cell(cell, kind, fig, user, others)
                    marks += n
                rebuilt.append(cell)
            lines[j] = "|" + "|".join(rebuilt) + "|"
            j += 1
        i = j
    return "\n".join(lines), marks


def _mark_cell(
    cell: str,
    kind: str,
    fig: Optional[EngineFigures],
    user: Dict[str, Set[float]],
    others: Dict[str, Set[float]],
) -> Tuple[str, int]:
    marks = 0
    pieces: List[str] = []
    pos = 0
    for m in _TOKEN_RE.finditer(cell):
        ends = _endpoints(m.group(0))
        pools: List[Iterable[float]] = [user.get(kind, ()), others.get(kind, ())]
        if fig is not None:
            pools += [fig.plan[kind], fig.channel[kind], fig.other[kind]]
        pieces.append(cell[pos : m.end()])
        pos = m.end()
        if ends and not _all_in(ends, *pools):
            pieces.append(DAGGER)
            marks += 1
    pieces.append(cell[pos:])
    return "".join(pieces), marks


# ---------------------------------------------------------------------------
# The guard
# ---------------------------------------------------------------------------
def _norm_dashes(s: str) -> str:
    return s.replace("–", "-").replace("—", "-")


def _projected_block(fig: EngineFigures) -> str:
    t = fig.text
    lines = ["**Projected outcomes (Nova planning engine)**"]
    if fig.reading:
        lines.append(f"- Budget: {fig.reading}")
    if t.get(APPS):
        lines.append(f"- Applications: {t[APPS]}")
    if HIRES in fig.suppressed:
        note = fig.labels.get("hires_note") or fig.labels.get("hires_short")
        if note:
            lines.append(f"- {note}")
    else:
        if t.get(HIRES) and fig.labels.get("hires_label"):
            lines.append(f"- {fig.labels['hires_label']}")
    if t.get(CPA):
        lines.append(f"- Cost per application: {t[CPA]}")
    if CPH not in fig.suppressed and t.get(CPH) and fig.labels.get("cph_label"):
        lines.append(f"- {fig.labels['cph_label']}")
    if any(fig.nondegenerate(k) for k in KINDS):
        lines.append(f"- {BEST_CASE_SENTENCE}")
    return "\n".join(lines)


def _basis_line(fig: EngineFigures) -> str:
    parts = []
    if HIRES in fig.suppressed:
        note = fig.labels.get("hires_note") or fig.labels.get("hires_short")
        if note:
            parts.append(note)
    else:
        if fig.labels.get("hires_short"):
            parts.append(fig.labels["hires_short"])
        if fig.labels.get("cph_short"):
            parts.append(f"Cost per hire: {fig.labels['cph_short']}.")
    if any(fig.nondegenerate(k) for k in KINDS):
        parts.append(BEST_CASE_SENTENCE)
    return "*Projection basis: " + " ".join(parts) + "*" if parts else ""


def enforce(
    response_text: str,
    tool_results: Sequence[Any],
    user_message: str = "",
    *,
    plan_context: bool = False,
    plan_shaped: bool = False,
) -> Tuple[str, Dict[str, int]]:
    """Hold the answer's plan figures to the engine's.

    * a hires / applications / CPA / cost-per-hire claim in prose that is the
      engine's range, a channel's own figure, a number the user typed or another
      tool's benchmark is left alone; a single number that is only an end of the
      engine's range, and any figure that matches nothing, becomes the engine's
      range (when exactly one projection is in the turn);
    * the same figure in a table cell, or with no projection to rewrite to, is
      marked with a dagger;
    * a plan answer that carries no engine range gets the engine's block, and a
      plan answer that quotes figures gets one basis / best-case line.

    Returns ``(text, {"rewritten", "marked", "appended"})``. Never raises.
    """
    stats = {"rewritten": 0, "marked": 0, "appended": 0}
    if not response_text or not isinstance(response_text, str):
        return response_text, stats
    try:
        return _enforce(
            response_text, tool_results, user_message, plan_context, plan_shaped, stats
        )
    except Exception as exc:  # noqa: BLE001 - the guard must never block an answer
        logger.warning(
            "nova_plan_figures: guard failed (non-fatal): %s", exc, exc_info=True
        )
        return response_text, {"rewritten": 0, "marked": 0, "appended": 0}


def _enforce(
    text: str,
    tool_results: Sequence[Any],
    user_message: str,
    plan_context: bool,
    plan_shaped: bool,
    stats: Dict[str, int],
) -> Tuple[str, Dict[str, int]]:
    engines, other_results = collect(tool_results)
    if not engines and not plan_context:
        return text, stats

    user = _user_pools(user_message)
    others: Dict[str, Set[float]] = {k: set() for k in KINDS}
    for res in other_results:
        _walk_kind_numbers(res, others)
    for fig in engines:
        for kind in KINDS:
            others[kind] |= fig.other[kind]

    # ONE truth per turn: the projection tool's own result outranks the headline
    # another tool stamped from a (possibly partial) brief of its own
    primaries = [f for f in engines if f.primary]
    engines = primaries or engines
    one = engines[0] if len(engines) == 1 else None
    merged: Optional[EngineFigures] = None
    if engines:
        merged = EngineFigures()
        for fig in engines:
            for kind in KINDS:
                merged.plan[kind] |= fig.plan[kind]
                merged.channel[kind] |= fig.channel[kind]
                merged.other[kind] |= fig.other[kind]
        # a single projection also gives the printable ranges / labels
        if one is not None:
            merged.text, merged.range = one.text, one.range
            merged.labels, merged.suppressed = one.labels, one.suppressed
            merged.reading, merged.budget = one.reading, one.budget

    # ---- prose claims ------------------------------------------------------
    edits: List[Tuple[int, int, str]] = []
    marks_at: List[int] = []
    quoted_kinds: Set[str] = set()
    for start, end, kind in _claims(text):
        ends = _endpoints(text[start:end])
        if not ends:
            continue
        cue = _has_channel_cue(text, start, end)
        pools_ok = [user[kind]]
        if kind in (CPA, CPH) or cue:
            # a cost benchmark from another tool is a fact about the market and
            # holds in any sentence; another tool's COUNT of hires or
            # applications (a single channel's funnel, a Meta-only estimate) is a
            # statement about that channel, never about the plan
            pools_ok.append(others.get(kind, ()))
            if merged is not None:
                pools_ok.append(merged.other[kind])
        if merged is not None and cue:
            pools_ok.append(merged.channel[kind])
        is_range = len(ends) > 1
        if _all_in(ends, *pools_ok):
            quoted_kinds.add(kind)
            continue
        bench = _has_benchmark_cue(text, start, end)
        if merged is not None and _all_in(ends, merged.plan[kind]):
            quoted_kinds.add(kind)
            if (
                not is_range
                and not bench
                and one is not None
                and one.rewritable(kind)
                and one.nondegenerate(kind)
            ):
                edits.append((start, end, one.text[kind]))
            continue
        # matches nothing
        quoted_kinds.add(kind)
        if one is not None and one.rewritable(kind) and not bench:
            edits.append((start, end, one.text[kind]))
        else:
            marks_at.append(end)

    out = text
    for start, end, repl in sorted(edits, reverse=True):
        out = out[:start] + repl + out[end:]
    # marks shift when edits happen: apply right-to-left on the original offsets
    # only when no edit precedes them (rare); otherwise recompute on the new text
    if marks_at:
        shift_edits = sorted(edits)
        for pos in sorted(marks_at, reverse=True):
            delta = sum(len(r) - (e - s) for s, e, r in shift_edits if e <= pos)
            at = pos + delta
            out = out[:at] + DAGGER + out[at:]
            stats["marked"] += 1
    stats["rewritten"] = len(edits)

    # ---- tables ------------------------------------------------------------
    out, table_marks = _mark_table_cells(out, merged, user, others)
    stats["marked"] += table_marks

    # ---- the engine's block / basis line ------------------------------------
    if one is not None:
        flat = _norm_dashes(out)
        has_range = any(
            len(_norm_dashes(one.text[k])) >= 4 and _norm_dashes(one.text[k]) in flat
            for k in KINDS
            if one.text.get(k)
        )
        if plan_shaped and not has_range and not one.solved:
            out = out.rstrip() + "\n\n" + _projected_block(one)
            stats["appended"] = 1
        elif (
            quoted_kinds & {HIRES, CPH}
            and "no client hire history" not in out
            and BEST_CASE_SENTENCE not in out
            and _basis_line(one)
            and _basis_line(one) not in out
        ):
            out = out.rstrip() + "\n\n" + _basis_line(one)
            stats["appended"] = 1
        if stats["rewritten"] and CORRECTED_LINE not in out:
            out = out.rstrip() + "\n\n" + CORRECTED_LINE
    return out, stats
