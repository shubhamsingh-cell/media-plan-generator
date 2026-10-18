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
* :func:`enforce` -- the answer-boundary guard, a NET THAT MARKS: a hires /
  applications / CPA / cost-per-hire figure the model typed that traces to no
  tool result of the turn and to no number the user typed (in any turn) gets a
  dagger. It never rewrites any text, in prose or in tables. A plan answer
  carries the engine's ranges with their basis and which end is the best case.

Pure functions, no Nova import, never raises into a caller.
"""

from __future__ import annotations

import bisect
import json
import logging
import re
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Sequence, Set, Tuple

logger = logging.getLogger(__name__)

DAGGER = "†"
CAP_NOTE = "*Some figures in this long answer were not checked.*"
BEST_CASE_SENTENCE = (
    "The high end of applications and hires, and the low end of cost per "
    "application and cost per hire, are the best case, if the campaign runs as "
    "efficiently as the plan allows; the other end is the industry average."
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
        # the campaign length the engine planned over (0 when the result omits it)
        self.months: float = 0.0

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
        if isinstance(br, dict):
            fig.months = _num(br.get("months")) or 0.0
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
#
# Policy (the guard is a net that marks, never an author): a hires /
# applications / CPA / cost-per-hire figure that traces to no tool result of the
# turn and to no number the user typed in any turn is followed by a dagger, in
# prose and in tables. NOTHING IS EVER REWRITTEN: a rewrite carries the engine's
# authority, and neither a sentence shape nor a table layout separates a plan
# total from a subset, a scenario, a rate or another budget reliably (four
# adversarial rounds each found shapes, then Total rows, that did not).
#
# Known limits (each would need a word list, so they are documented instead):
#   * a channel word anywhere in the clause lets a figure equal to ANY channel's
#     own figure pass ("This plan delivers 12 hires, led by programmatic" when
#     programmatic models 12);
#   * a per-period rate is checked only when its clause names the plan ("That's
#     about 20 hires a month" passes);
#   * an answer in another language claims nothing (no unit word is recognised).
# ---------------------------------------------------------------------------
# every dash a range can be written with: hyphen, U+2010 hyphen, U+2011
# non-breaking hyphen, U+2012 figure dash, en and em dash, U+2212 minus
_DASH = "[‐‑‒–—−-]"
_NUM = (
    r"(?:[1-9]\d{0,2}(?:\.\d{3})+,\d+"  # 1.234,56: a decimal comma
    r"|\d{1,2}(?:,\d{2})+,\d{3}(?!\d)"  # 12,34,567: lakh grouping
    r"|\d{1,3}(?:,\d{3})+(?!\d)"
    r"|\d{1,3}(?:[    ]\d{3})+(?!\d)"
    r"|\d+)(?:\.\d+)?"
)
_MAG = r"(?:\s?(?:[kKmM]|thousand|million|lakhs?|crores?)(?![A-Za-z]))"
_SCALE = {
    "k": 1e3, "thousand": 1e3, "m": 1e6, "million": 1e6,
    "lakh": 1e5, "lakhs": 1e5, "crore": 1e7, "crores": 1e7,
}  # fmt: skip
_CUR_PRE = r"(?:US\$|[CA]\$|[$€£₹¥]|(?:USD|EUR|GBP|INR|JPY|CAD|AUD)\s?)"
_CUR_POST = (
    r"(?:\s?(?:USD|EUR|GBP|INR|JPY|CAD|AUD|dollars?|euros?|pounds?|rupees?|yen)"
    r"(?![A-Za-z])|\s?[€£])"
)
# a currency other than the plan's (the engine prices in US dollars)
_FOREIGN_CUR_RE = re.compile(
    r"[€£₹¥]|[CA]\$|\b(?:EUR|GBP|INR|JPY|CAD|AUD|euros?|pounds?|rupees?|yen)\b"
)
_COUNT = rf"~?{_NUM}{_MAG}?"
_MONEY = rf"(?<![A-Za-z])~?(?:{_CUR_PRE}\s?{_NUM}{_MAG}?|{_NUM}{_MAG}?{_CUR_POST})"
_MONEY_NEXT = rf"~?(?:{_CUR_PRE}\s?)?{_NUM}{_MAG}?(?:{_CUR_POST})?"
# "17-35", "17 to 35", "between 17 and 35": ONE range token
_SEP = rf"\s*(?:{_DASH}|to|through|thru|and)\s*"
# A value is never a piece of a longer token: not of a number ("12%" is not "1"),
# a phone number or an id ("1-800-555-0199"), a date ("11/30")
_PRE_GUARD = rf"(?<![\d.,])(?<!\d{_DASH})"
_POST_GUARD = (
    rf"(?!\d|[.,]\d|{_DASH}\d|/\d|[xX×]\b|\s*(?:%|percent\b|per\s?cent\b|pct\b))"
)
_VAL_COUNT = rf"(?P<val>{_PRE_GUARD}{_COUNT}(?:{_SEP}{_COUNT})?){_POST_GUARD}"
_VAL_MONEY = rf"(?P<val>{_PRE_GUARD}{_MONEY}(?:{_SEP}{_MONEY_NEXT})?){_POST_GUARD}"
_W = r"(?:\*{1,2}|_{1,2})?"  # markdown emphasis around a figure or its unit
_SUP = r"[¹²³⁰-⁹]*"  # a footnote mark ("900¹")
_ADJ = (
    rf"(?:{_W}(?:projected|estimated|expected|total|new|additional|qualified|"
    rf"full[- ]time|net){_W}\s+)*"
)
_LEAD = r"(?:(?:of|is|are|at|:|=|around|about|approximately|roughly|averag\w*|totall?ing|ranges?\s+from|between|~)(?:\*\*|__)?)?\s*(?:\*\*|__)?"
_COUNT_LEAD = r"(?:of|:|=|\s[—–]|are|is|at|totall?ing)(?:\*\*|__)?\s*(?:\*\*|__)?(?:about\s+|around\s+|approximately\s+|roughly\s+|~\s*)?"
_NB = r"(?<![\d$.,/])"
# a qualifier between a label and its value: "Projected hires (supervisors only): 5",
# "Projected hires — supervisors: 5"
_LABEL_QUAL = r"(?:\s*\([^)\n]{0,40}\)|\s*[—–]\s*[^:\n]{1,30}(?=:))?"
_APPS_UNIT = r"(?:applications?|applies|apply[- ]starts?|apps)"
# a label, then up to three words before the amount ("CPA works out to $66")
_TO_MONEY = rf"{_W}\s*(?:\([^)\n]{{0,30}}\)\s*)?(?:[a-z]+\s+){{0,3}}?{_LEAD}"
_CPA_WORDS = r"(?:CPA|cost[- ]per[- ](?:application|applicant|apply)|per[- ]appli(?:cation|cant)\s+cost)"
_CPH_WORDS = r"(?:CPH|cost[- ](?:per|to)[- ](?:new[- ])?hire)"

# number words: "N hundred / N thousand", "a dozen" and ten and above ("one" ..
# "nine" alone are not plan figures). They are claims too, and only marked.
_UNITS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7,
    "eight": 8, "nine": 9,
}  # fmt: skip
_TEENS = {
    "ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14,
    "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19,
}  # fmt: skip
_TENS = {
    "twenty": 20, "thirty": 30, "forty": 40, "fifty": 50, "sixty": 60,
    "seventy": 70, "eighty": 80, "ninety": 90,
}  # fmt: skip
_W_TEENS = "|".join(sorted(_TEENS, key=len, reverse=True))
_W_TENS_UNITS = rf"(?:{'|'.join(_TENS)})(?:[- ](?:{'|'.join(_UNITS)}))?"
_W_BELOW100 = rf"(?:{_W_TEENS}|{_W_TENS_UNITS}|{'|'.join(_UNITS)})"
_WORDNUM = (
    rf"\b(?:(?:an?|{_W_BELOW100})[\s-]+(?:hundred|thousand)(?:[\s-]+(?:and\s+)?{_W_BELOW100})?"
    rf"|(?:an?|{_W_BELOW100})\s+dozen|{_W_TEENS}|{_W_TENS_UNITS})\b"
)
_WORD_RE = re.compile(
    r"\b(?:"
    + "|".join(list(_UNITS) + list(_TEENS) + list(_TENS))
    + r"|hundred|thousand|dozen)\b",
    re.IGNORECASE,
)


def _word_value(text: str) -> int:
    """ "nine hundred" -> 900, "twenty-five" -> 25, "a hundred" -> 100,
    "two dozen" -> 24, "two thousand four hundred" -> 2400."""
    total = cur = 0
    for tok in re.split(r"[-\s]+", text.lower()):
        if tok in _UNITS:
            cur += _UNITS[tok]
        elif tok in _TEENS:
            cur += _TEENS[tok]
        elif tok in _TENS:
            cur += _TENS[tok]
        elif tok in ("a", "an"):
            cur = max(cur, 1)
        elif tok == "hundred":
            cur = max(cur, 1) * 100
        elif tok == "dozen":
            cur = max(cur, 1) * 12
        elif tok == "thousand":
            total += max(cur, 1) * 1000
            cur = 0
    return total + cur


# "placements" (ad placements) is not a plan unit; the verb "hires about 40,000"
# is not a count of hires
_CLAIM_RES: Dict[str, List["re.Pattern[str]"]] = {
    HIRES: [
        re.compile(rf"{_NB}{_W}{_VAL_COUNT}{_W}{_SUP}\s*\+?\s*{_ADJ}{_W}hires?\b", re.I),
        re.compile(rf"\bhires?{_W}{_LABEL_QUAL}\s*{_COUNT_LEAD}{_W}{_VAL_COUNT}", re.I),
    ],
    APPS: [
        re.compile(rf"{_NB}{_W}{_VAL_COUNT}{_W}{_SUP}\s*{_ADJ}{_W}{_APPS_UNIT}\b", re.I),
        re.compile(rf"\b(?:applications?|applies){_W}{_LABEL_QUAL}\s*{_COUNT_LEAD}{_W}{_VAL_COUNT}", re.I),
    ],
    CPA: [
        re.compile(rf"\b{_CPA_WORDS}\b{_TO_MONEY}{_VAL_MONEY}", re.I),
        re.compile(rf"{_VAL_MONEY}{_W}\s*(?:(?:avg\.?|average|blended)\s+)?(?:CPA\b|per[- ](?:application|apply|applicant)\b|cost[- ]per[- ](?:application|applicant|apply)\b|/\s?(?:application|applicant)\b)", re.I),
    ],
    CPH: [
        re.compile(rf"\b{_CPH_WORDS}\b{_TO_MONEY}{_VAL_MONEY}", re.I),
        re.compile(rf"{_VAL_MONEY}{_W}\s*(?:(?:avg\.?|average|blended)\s+)?(?:CPH\b|(?:per|a|each)\s+(?:new\s+)?hire\b|/\s?hire\b|cost[- ]per[- ]hire\b)", re.I),
        re.compile(rf"\b(?:each|every|per)\s+(?:new\s+)?hire\s+(?:will\s+|would\s+|should\s+|could\s+|may\s+)?costs?\s+{_LEAD}{_VAL_MONEY}", re.I),
    ],
}  # fmt: skip
_WORD_CLAIM_RES: Dict[str, "re.Pattern[str]"] = {
    HIRES: re.compile(rf"{_NB}(?P<val>{_WORDNUM})(?![\w-])\s*{_ADJ}hires?\b", re.I),
    APPS: re.compile(
        rf"{_NB}(?P<val>{_WORDNUM})(?![\w-])\s*{_ADJ}{_APPS_UNIT}\b", re.I
    ),
}
_TOKEN_RE = re.compile(rf"~?(?:{_CUR_PRE}\s?)?(?P<n>{_NUM})(?P<s>{_MAG})?")


class Claim(NamedTuple):
    vs: int  # the number or range itself
    ve: int
    ms: int  # the whole match, unit word included
    me: int
    kind: str
    words: bool = False


def _parse_num(raw: str) -> Optional[Tuple[float, int]]:
    """(value, decimals) of one written number: "1,234", "1 234", "1.234,56",
    "12,34,567", and "7.500" (three-digit groups after a dot are thousands)."""
    s = re.sub(r"[    ]", "", raw)
    if re.fullmatch(r"[1-9]\d{0,2}(?:\.\d{3})+,\d+", s):
        s = s.replace(".", "").replace(",", ".")
    elif re.fullmatch(r"[1-9]\d{0,2}(?:\.\d{3})+", s):
        s = s.replace(".", "")
    else:
        s = s.replace(",", "")
    try:
        value = float(s)
    except ValueError:
        return None
    return value, (len(s.split(".")[1]) if "." in s else 0)


def _endpoints(val: str) -> List[Tuple[float, int, float]]:
    """[(value, decimals, scale)] for "17-35", "$8-$25", "2.5k-4k", "fourteen"."""
    if not re.search(r"\d", val) and _WORD_RE.search(val):
        v = _word_value(val)
        return [(float(v), 0, 1.0)] if v else []
    out: List[Tuple[float, int, float]] = []
    for m in _TOKEN_RE.finditer(val):
        parsed = _parse_num(m.group("n"))
        if parsed is None:
            continue
        suffix = (m.group("s") or "").strip().lower()
        out.append((parsed[0], parsed[1], _SCALE.get(suffix, 1.0)))
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
    return {v * s for v, _d, s in _endpoints(message or "")}


# ---- sentence, clause and span helpers --------------------------------------
_SENT_END_RE = re.compile(r"[.!?](?=\s|$)|\n")
# a clause break inside a sentence: a figure's context is read in ITS clause
_CLAUSE_SPLIT_RE = re.compile(
    r";|[()]|\s[—–]\s|—|\b(?:but|while|whereas|although|though)\b|,\s*(?:so|which|meaning)\b|"
    r"\bi\.e\.|\be\.g\.",
    re.IGNORECASE,
)


class _Index:
    """Sentence and clause boundaries of one answer, found ONCE: locating a
    claim's clause is a bisect, and a clause's context is read once however
    many claims it holds (an answer's cost is linear in its length)."""

    def __init__(self, text: str, budgets: Sequence[float] = ()) -> None:
        self.text = text
        self.budgets = [b for b in budgets if b]
        self.s_starts: List[int] = []
        self.s_ends: List[int] = []
        for m in _SENT_END_RE.finditer(text):
            self.s_starts.append(m.start())
            self.s_ends.append(m.end())
        self.c_starts: List[int] = []
        self.c_ends: List[int] = []
        for m in _CLAUSE_SPLIT_RE.finditer(text):
            self.c_starts.append(m.start())
            self.c_ends.append(m.end())
        self.memo: Dict[Tuple[int, int], Dict[str, bool]] = {}

    def sentence(self, start: int, end: int) -> Tuple[int, int]:
        k = bisect.bisect_right(self.s_ends, start)
        left = self.s_ends[k - 1] if k else 0
        n = bisect.bisect_left(self.s_starts, end)
        return left, (self.s_ends[n] if n < len(self.s_ends) else len(self.text))

    def clause(self, start: int, end: int) -> Tuple[int, int, int]:
        """(left, right, right with a qualifier) of the clause holding
        text[start:end]: its sentence, cut at ';', parentheses, dashes and
        contrast words. The wider right end also takes in a parenthetical or dash
        qualifier right after the figure ("14 hires (programmatic only)")."""
        sa, sb = self.sentence(start, end)
        left = sa
        k = bisect.bisect_right(self.c_ends, start) - 1
        if k >= 0 and self.c_starts[k] >= sa:
            left = self.c_ends[k]
        right = sb
        j = bisect.bisect_left(self.c_starts, end)
        if j < len(self.c_starts) and self.c_starts[j] < sb:
            right = self.c_starts[j]
        wide = right
        if right < sb:
            tail = re.match(
                r"\s*(?:\([^)\n]{0,80}\)?|\s[—–]\s[^\n]{0,80}|—[^\n]{0,80})",
                self.text[right : min(sb, right + 90)],
            )
            if tail:
                wide = min(sb, right + tail.end())
        return left, right, wide

    def cues(self, ca: int, cb: int, wide: int) -> Dict[str, bool]:
        key = (ca, wide)
        hit = self.memo.get(key)
        if hit is None:
            core, clause = self.text[ca:cb], self.text[ca:wide]
            hit = {
                "skip": bool(_SKIP_RE.search(core)),
                "dated": bool(_YEAR_RE.search(core) and _PAST_RE.search(core)),
                "non_job": bool(_NON_JOB_APPS_RE.search(core)),
                "accountant": bool(_ACCOUNTANT_RE.search(core)),
                "plan": bool(_PLAN_NOUN_RE.search(clause))
                or self._names_engine_budget(clause),
                "expect": bool(_EXPECT_RE.search(clause)),
                "third": bool(
                    _THIRD_PARTY_RE.search(clause)
                    or _THIRD_PARTY_NAME_RE.search(clause)
                ),
                "bench": bool(_BENCH_CUE_RE.search(clause)),
                "channel": bool(_CHANNEL_CUE_RE.search(clause)),
            }
            self.memo[key] = hit
        return hit

    def _names_engine_budget(self, clause: str) -> bool:
        """The clause names the projection's own budget ("for $60k, ...")."""
        for m in re.finditer(_MONEY, clause):
            for v, _d, s in _endpoints(m.group(0))[:1]:
                if any(abs(v * s - b) <= 0.01 * b for b in self.budgets):
                    return True
        return False


_PROTECT_RE = re.compile(
    r"```.*?(?:```|\Z)"  # fenced code (an unclosed fence runs to the end)
    r"|`[^`\n]+`"  # inline code
    r"|(?:https?://|www\.)\S+"  # URLs
    r"|\[[^\]\n]{0,200}\]\([^)\s]*\)"  # a markdown link: its text and its target
    r"|[?&][\w.\-]+=[^\s&]*"  # a bare query string
    r'|"[^"\n]{6,240}"|“[^”\n]{6,240}”|‘[^‘’\n]{6,240}’',  # quoted copy
    re.DOTALL,
)


def _protected_spans(text: str) -> List[Tuple[int, int]]:
    """Code fences, inline code, URLs, query strings and quoted copy: nothing in
    them is the model's claim about the plan."""
    return [(m.start(), m.end()) for m in _PROTECT_RE.finditer(text)]


def _inside(pos: int, spans: Sequence[Tuple[int, int]]) -> bool:
    k = bisect.bisect_right(spans, (pos, float("inf"))) - 1
    return k >= 0 and spans[k][0] <= pos < spans[k][1]


# ---- context: is a figure a statement about the plan at all? ----------------
# a single channel or publisher: its own figure (from the tool) is true of it
_CHANNEL_CUE_RE = re.compile(
    r"\b(?:programmatic|dsp|indeed|linkedin|ziprecruiter|zip\s?recruiter|glassdoor|google|bing|"
    r"meta|facebook|instagram|tiktok|snapchat|social|niche|regional|global|"
    r"employer\s+branding|craigslist|monster|careerbuilder|snagajob|handshake|jooble|"
    r"programmatic_dsp|global_boards|niche_boards|social_media|regional_boards|employer_branding|"
    r"(?:per|each|by|single|one)\s+(?:channel|board|publisher)|channel[- ]level|alone|only)\b",
    re.IGNORECASE,
)
# a benchmark or market statement
_BENCH_CUE_RE = re.compile(
    r"\b(?:benchmarks?|typical(?:ly)?|norms?|median|shrm|appcast|bls|historically|generally|usually|"
    r"industry[- ](?:average|range|standard|rate)|national(?:ly)?|nationwide|market (?:rate|average)|"
    r"gross cpa|net cpa|on average)\b",
    re.IGNORECASE,
)
# another entity (an agency, a competitor, another employer) or a past period
_THIRD_PARTY_RE = re.compile(
    r"\b(?:agenc(?:y|ies)|staffing|competitors?|other\s+employers?|companies|"
    r"last\s+(?:year|month|quarter|week|season|fall|spring|summer|winter)|previous(?:ly)?|prior|"
    r"earlier|historical(?:ly)?|in\s+the\s+past)\b",
    re.IGNORECASE,
)
_THIRD_PARTY_NAME_RE = re.compile(
    r"\b(?!(?:Nova|Your|This|That|These|Those|Our|The|You|We)\b)[A-Z][a-z]{2,}\s+"
    r"(?:made|hired|announced|plans?|posted|added|cut|laid|employs?|runs?)\b"
)
# the clause is about THIS plan ("you" / "your" alone do not count)
_PLAN_NOUN_RE = re.compile(
    r"\b(?:the|this|your|our|that)\s+(?:\$?[\d,.]+\s*[kKmM]?\s+)?(?:media\s+)?(?:plan|campaign|budget)\b|\bnova\b",
    re.IGNORECASE,
)
_EXPECT_RE = re.compile(
    r"\b(?:expect\w*|estimate\w*|project\w*|forecast\w*)\b", re.IGNORECASE
)
# a part of a whole ("40% of 900 applications", "half of the 14 hires")
_OF_BEFORE_RE = re.compile(
    r"(?:%|\b(?:half|third|quarter|fifth|tenth|most|all|some|many|few|none|both|any))\s+of\s+"
    r"(?:the\s+|those\s+|these\s+|your\s+|our\s+)?$",
    re.IGNORECASE,
)
# structural: a ratio, an illustration, a denominator, a per-unit figure
_SKIP_RE = re.compile(
    r"\b(?:clicks?|impressions?|click[- ]to[- ]apply|conversion|apply[- ]rate|hire[- ]rate|ratio|funnel|"
    r"for\s+every|out\s+of|\d+\s+in\s+\d+|(?:to|per)\s+(?:1|one)\s+(?:hire|applica\w+|applicant)|"
    r"per\s+(?:click|impression|opening|candidate|recruiter|role|hour|"
    r"requisition|posting|position|vacancy|\d+)|"
    r"(?:for|to\s+fill|to\s+make|to\s+land|to\s+close|behind|before\s+you\s+land|per)\s+"
    r"(?:each|every|a\s+single|one|a|any)\s+(?:\w+\s+){0,2}"
    r"(?:hire|role|opening|position|requisition|req|posting|job|vacancy|placement)|"
    r"(?:each|every|a\s+single)\s+(?:\w+\s+){0,2}(?:requisition|req|posting|opening|job|role|position|vacancy)|"
    r"(?:each|every)\s+(?:\w+\s+){0,3}(?:handl|process|clos|manag|screen|interview|support|fill|source|own|carry)\w*|"
    r"behind\s+every|before\s+you\s+land|"
    r"for\s+example|for\s+instance|e\.g\.|divided\s+by|suppose|imagine|hypothetical|say\s+you)\b",
    re.IGNORECASE,
)
_NON_JOB_APPS_RE = re.compile(
    r"\b(?:loan|mortgage|credit|visa|permit|patent|college|school|admission|software|ats|integrat\w*|"
    r"workday|greenhouse|taleo|icims|erp|saas|mobile|web|ios|android|app\s+store|download\w*)\b",
    re.IGNORECASE,
)
_ACCOUNTANT_RE = re.compile(
    r"\b(?:salary|salaries|wage|wages|an\s+hour|per\s+hour|/\s?h(?:ou)?r|hourly|licensed|accountant|exam|"
    r"audit|tax|senior|junior|years?'?\s+(?:of\s+)?experience)\b",
    re.IGNORECASE,
)
# a dated reference: a year, a quarter, a fiscal period, "last campaign"
_YEAR_RE = re.compile(
    r"(?<![\d,.$])(?:19|20)\d\d(?![\d,]|\.\d)|\bQ[1-4]\b|\b(?:FY|H[12])\s?\d*\b"
    r"|\blast\s+(?:campaign|plan|cycle|time|quarter)\b",
    re.IGNORECASE,
)
_PAST_RE = re.compile(
    r"\b(?:made|had|hired|got|received|logged|produced|saw|ran|spent|brought|delivered|generated|drew)\b",
    re.IGNORECASE,
)
_DENOM_AFTER_RE = re.compile(
    r"^\s*(?:per\b|/|for\s+every\b|of\b|each\b|every\b|a\s+(?:opening|recruiter|role)\b|"
    r"an\s+hour\b)",
    re.IGNORECASE,
)
_DENOM_BEFORE_RE = re.compile(
    r"(?:\bper|\bfor\s+every|\beach|\bevery|/)\s*$", re.IGNORECASE
)
# a count per period ("40 hires a month", "1,000 applications every week"): a
# rate, checked against the engine's range spread over the campaign's length
_RATE_AFTER_RE = re.compile(
    r"^[*_]{0,2}\s*(?:(?:per|a|an|each|every|/)\s*(?P<p>day|week|month|quarter|year)\b"
    r"|(?P<adv>dai|week|month|quarter|annual|year)ly\b)",
    re.IGNORECASE,
)
_PERIODS_PER_MONTH = {
    "day": 365.0 / 12, "week": 52.0 / 12, "month": 1.0, "quarter": 1.0 / 3,
    "year": 1.0 / 12,
}  # fmt: skip
_RATE_TOLERANCE = 0.10

# "$60k a month" / "$60k per month": a budget stated per period
_PERIOD_BUDGET_RE = re.compile(
    rf"(?P<amt>{_CUR_PRE}\s?{_NUM}{_MAG}?)\s*(?:a|per|each|every|/)\s*"
    r"(?:month|mo|quarter|qtr|year|yr|week)\b",
    re.IGNORECASE,
)
_BARE_INT_RE = re.compile(
    r"(?<![\w$.,])(?P<n>\d{1,3}(?:,\d{3})+|\d+)(?![\w.,]?\d)(?!\s*(?:%|k\b|m\b|x\b|"
    r"(?:month|week|day|year|quarter|hour|min|st|nd|rd|th)s?\b|-\s?(?:month|week|day|year|quarter)))",
    re.IGNORECASE,
)
# a budget anywhere in the conversation: "$60k", "€200,000", "60k"
_BUDGET_RE = re.compile(rf"{_MONEY}|(?<![\w.,]){_NUM}\s?[kKmM](?![A-Za-z])")


def user_turn_texts(user_message: str, conversation_history: Any = None) -> List[str]:
    """Every user turn: earlier ones from the history, then the current message."""
    out: List[str] = []
    for item in conversation_history or []:
        if not isinstance(item, dict) or str(item.get("role") or "") != "user":
            continue
        content = item.get("content")
        if isinstance(content, str):
            out.append(content)
        elif isinstance(content, list):
            out.extend(
                str(b.get("text") or "")
                for b in content
                if isinstance(b, dict) and b.get("type") == "text"
            )
    out.append(user_message or "")
    return out


def _user_pools(messages: Any) -> Dict[str, Set[float]]:
    """The figures the USER stated, by kind, from EVERY user turn of the
    conversation, so an echo ("your goal of 40 hires", "your 25 openings", "a team
    of fourteen", "a hundred hires", "two dozen") is never read as an invention. A
    count is any bare integer or number word in a user message that is not a
    percentage or a duration ("3-month" does not make "3 hires" an echo); money
    amounts count as typed."""
    if isinstance(messages, str):
        messages = [messages]
    pools: Dict[str, Set[float]] = {k: set() for k in KINDS}
    for msg in messages or []:
        msg = msg or ""
        counts: Set[float] = set()
        for m in _BARE_INT_RE.finditer(msg):
            try:
                counts.add(float(m.group("n").replace(",", "")))
            except ValueError:
                continue
        for m in re.finditer(_WORDNUM, msg, re.IGNORECASE):
            v = _word_value(m.group(0))
            if v:
                counts.add(float(v))
        pools[HIRES] |= counts
        pools[APPS] |= counts
        if re.search(r"\bcp[ah]\b|cost[- ]per", msg, re.IGNORECASE):
            pools[CPA] |= counts
            pools[CPH] |= counts
        for c in _claims(msg):
            for value, _d, scale in _endpoints(msg[c.vs : c.ve]):
                pools[c.kind].add(value * scale)
        money = {
            value * scale
            for tok in re.findall(_MONEY, msg)
            for value, _d, scale in _endpoints(tok)
        }
        # a typed amount that equals a count ("under $53 a hire" and "53 hires")
        # is still the user's own number
        for kind in KINDS:
            pools[kind] |= money
    return pools


def _conversation_names_a_budget(messages: Sequence[str]) -> bool:
    for msg in messages:
        for m in _BUDGET_RE.finditer(msg or ""):
            if any(v * s >= 1000 for v, _d, s in _endpoints(m.group(0))):
                return True
    return False


def _claims(text: str) -> List[Claim]:
    """Every plan-figure claim, non-overlapping, in order."""
    found: List[Claim] = []
    for kind, patterns in _CLAIM_RES.items():
        for rx in patterns:
            for m in rx.finditer(text):
                found.append(
                    Claim(m.start("val"), m.end("val"), m.start(), m.end(), kind)
                )
    for kind, rx in _WORD_CLAIM_RES.items():
        for m in rx.finditer(text):
            if _word_value(m.group("val")) >= 10:
                found.append(
                    Claim(m.start("val"), m.end("val"), m.start(), m.end(), kind, True)
                )
    found.sort(key=lambda c: (c.vs, -(c.ve - c.vs)))
    out: List[Claim] = []
    last_end = -1
    for c in found:
        if c.vs < last_end:
            continue
        out.append(c)
        last_end = c.ve
    return out


def _is_year(val: str) -> bool:
    """A year, or a range of years ("2025 and 2027"): a date, not a count."""
    tokens = re.findall(r"[\d,]+", val)
    return bool(tokens) and all(re.fullmatch(r"(?:19|20|21)\d\d", t) for t in tokens)


def _rate_period(text: str, claim: Claim) -> str:
    """'day' / 'week' / 'month' / 'quarter' / 'year' when a count is stated per
    period ("40 hires a month", "3 hires weekly"), else ''."""
    if claim.kind not in (HIRES, APPS):
        return ""
    m = _RATE_AFTER_RE.match(text[claim.me : claim.me + 24])
    if not m:
        return ""
    period = (m.group("p") or m.group("adv")).lower()
    return {"dai": "day", "annual": "year"}.get(period, period)


def _not_a_plan_claim(
    text: str,
    claim: Claim,
    ends: List[Tuple[float, int, float]],
    cues: Dict[str, bool],
    rate: str,
) -> bool:
    """Contexts in which a number beside a hire / application / CPA word is not a
    statement about the plan at all (left byte-for-byte alone, not even marked):
    a ratio ("45 applications for each hire"), an illustration, a percentage, a
    year or a dated past statement, a decimal count, funnel arithmetic, software
    or loan "applications", an accountant's CPA."""
    val = text[claim.vs : claim.ve]
    kind = claim.kind
    if kind in (HIRES, APPS):
        if _is_year(val):
            return True
        if any(d and s == 1.0 for _v, d, s in ends):
            return True  # "3.4 applications": an average, not a plan total
        if not rate and _DENOM_AFTER_RE.match(text[claim.me : claim.me + 40]):
            return True
        if _DENOM_BEFORE_RE.search(text[max(0, claim.vs - 14) : claim.vs]):
            return True
        if _OF_BEFORE_RE.search(text[max(0, claim.vs - 24) : claim.vs]):
            return True
        if kind == APPS and cues["non_job"]:
            return True
    else:
        if re.match(
            r"^\s*(?:per\s+(?:click|impression|day|week|month|year|hour|opening)\b|/\s?(?:day|week|month|year|hour|click)\b)",
            text[claim.me : claim.me + 40],
            re.IGNORECASE,
        ):
            return True
        if kind == CPA and cues["accountant"]:
            return True
    if re.match(r"^\s*%", text[claim.ve : claim.ve + 3]):
        return True
    return cues["skip"] or cues["dated"]


_DURATION_RE = re.compile(
    r"(?<![\d.])(\d+(?:\.\d+)?)\s*-?\s*(month|week|year)s?\b", re.IGNORECASE
)
_MONTHS_PER = {"month": 1.0, "week": 12.0 / 52, "year": 12.0}


def _campaign_months(
    engines: Sequence[EngineFigures], turns: Sequence[str]
) -> Set[float]:
    """The campaign lengths a rate may be spread over: the engine's own, and any
    length the user typed ("3-month", "12 weeks")."""
    out = {f.months for f in engines if f.months}
    for msg in turns:
        for m in _DURATION_RE.finditer(msg or ""):
            out.add(float(m.group(1)) * _MONTHS_PER[m.group(2).lower()])
    return {m for m in out if m > 0}


def _rate_consistent(
    ends: List[Tuple[float, int, float]],
    kind: str,
    period: str,
    engines: Sequence[EngineFigures],
    months: Set[float],
) -> bool:
    """A per-period count is the engine's when it is the engine's range spread
    over the campaign's months (within a tenth either way)."""
    for fig in engines:
        lo, hi = fig.range.get(kind, (None, None))
        if lo is None or hi is None:
            continue
        for length in months:
            n = length * _PERIODS_PER_MONTH[period]
            if all(
                lo / n * (1 - _RATE_TOLERANCE)
                <= v * s
                <= hi / n * (1 + _RATE_TOLERANCE)
                for v, _d, s in ends
            ):
                return True
    return False


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------
_TABLE_KIND_RES = (
    (CPH, re.compile(r"\bcph\b|cost[ /]*(?:per[ /]*)?hire", re.I)),
    (CPA, re.compile(r"\bcpa\b|cost[ /]*(?:per[ /]*)?app", re.I)),
    (APPS, re.compile(r"\bapplications?\b|\bapplies\b|\bapps\b", re.I)),
    (HIRES, re.compile(r"\bhires?\b", re.I)),
)
_SEP_ROW_RE = re.compile(r"^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)*\|?\s*$")
# a ratio or a rate, not a count ("Applications per hire", "Apply rate")
_RATIO_LABEL_RE = re.compile(
    r"\b(?:per|rate|ratio|conversion|click|impression|funnel|share|mix)\b|%|/", re.I
)
# a total or overall row states a plan-level figure: only the engine's own plan
# figures and the user's numbers excuse it, never one channel's
_TOTAL_LABEL_RE = re.compile(r"^(?:grand |plan )?(?:totals?|overall)\b")
_PURE_VALUE_RE = re.compile(
    rf"^{_W}~?(?:{_CUR_PRE}\s?)?{_NUM}{_MAG}?(?:{_CUR_POST})?"
    rf"(?:\s*(?:{_DASH}|to)\s*~?(?:{_CUR_PRE}\s?)?{_NUM}{_MAG}?(?:{_CUR_POST})?)?{_W}$"
)
# a phone number or an id ("1-800-555-0199", "2026-11-01"): never a figure
_ID_RUN_RE = re.compile(rf"\d{_DASH}\d+{_DASH}\d")


def _plain_label(cell: str) -> str:
    return (
        re.sub(r"\s+", " ", re.sub(r"[*_]", "", cell))
        .strip()
        .rstrip(":")
        .strip()
        .lower()
    )


def _table_kind(header: str) -> Optional[str]:
    for kind, rx in _TABLE_KIND_RES:
        if rx.search(header):
            if kind in (APPS, HIRES) and _RATIO_LABEL_RE.search(header):
                return None
            return kind
    return None


def _row_cells(line: str) -> Tuple[List[str], List[int]]:
    """The pieces of ``line`` split at '|' and the indexes of its cells, so a row
    is rebuilt with its own indentation, padding and (missing) trailing pipe."""
    parts = line.split("|")
    last = len(parts) - 1 if line.rstrip().endswith("|") else len(parts)
    return parts, list(range(1, last))


# The per-answer work budget: claims and table cells checked (protected claims,
# in code or quotes, do not count). Past it nothing further is checked and the
# answer says so (CAP_NOTE); a total row is always checked first.
MAX_CHECKS_PER_ANSWER = 5000


def _mark_table_cells(
    text: str,
    fig: Optional[EngineFigures],
    user: Dict[str, Set[float]],
    others: Dict[str, Set[float]],
    budget: int,
) -> Tuple[str, int, bool]:
    """Mark every Hires / CPA / CPH / Applications number in a markdown table
    that is not the engine's, the user's or (in one channel's row) that
    channel's own or another tool's cost benchmark, whichever way the table is
    laid out: the kind is named by the column header ("| Channel | Hires |") or
    by the row's first cell ("| Hires | 14 |", the Metric | Value layout). A row
    labelled as a total ("Total", "Totals", "Overall", "Total (all roles)") and
    a Metric | Value row state plan-level figures. Total rows are checked before
    any other row, so the work cap never skips them. Tables in code are
    skipped. Returns (text, marks, whether the cap left cells unchecked)."""
    protect = _protected_spans(text)
    lines = text.split("\n")
    offsets: List[int] = []
    pos = 0
    for line in lines:
        offsets.append(pos)
        pos += len(line) + 1
    rows: List[Tuple[bool, int, List[Optional[str]]]] = []  # (total?, line, kinds)
    i = 0
    while i < len(lines) - 1:
        if not lines[i].lstrip().startswith("|") or not _SEP_ROW_RE.match(lines[i + 1]):
            i += 1
            continue
        in_code = _inside(offsets[i] + len(lines[i]) - len(lines[i].lstrip()), protect)
        hparts, hidx = _row_cells(lines[i])
        col_kinds = [_table_kind(hparts[k].strip()) for k in hidx]
        j = i + 2
        while j < len(lines) and lines[j].lstrip().startswith("|"):
            if not in_code:
                parts, idx = _row_cells(lines[j])
                label = parts[idx[0]] if idx else ""
                rows.append(
                    (bool(_TOTAL_LABEL_RE.match(_plain_label(label))), j, col_kinds)
                )
            j += 1
        i = j
    marks = 0
    capped = False
    # total rows first: the plan-level claim of a long table is in its last row
    for is_total, j, col_kinds in sorted(rows, key=lambda r: (not r[0], r[1])):
        parts, idx = _row_cells(lines[j])
        row_kind = _table_kind(parts[idx[0]]) if idx else None
        for n, k in enumerate(idx):
            col_kind = col_kinds[n] if n < len(col_kinds) else None
            kind = col_kind or row_kind
            if n == 0 or not kind or DAGGER in parts[k]:
                continue
            if not is_total:
                budget -= 1
                if budget < 0:
                    capped = True
                    break
            parts[k], mk = _check_cell(
                parts[k], kind, fig, user, others, is_total, col_kind is None
            )
            marks += mk
        lines[j] = "|".join(parts)
    return "\n".join(lines), marks, capped


def _check_cell(
    cell: str,
    kind: str,
    fig: Optional[EngineFigures],
    user: Dict[str, Set[float]],
    others: Dict[str, Set[float]],
    is_total: bool,
    metric_row: bool,
) -> Tuple[str, int]:
    """One table cell: unchanged or marked (never rewritten)."""
    if _ID_RUN_RE.search(cell):
        return cell, 0
    pools: List[Iterable[float]] = [user.get(kind, ())]
    if fig is not None:
        pools.append(fig.plan[kind])
    if not (is_total or metric_row):  # one channel's row: its own figure
        pools.append(others.get(kind, ()))
        if fig is not None:
            pools += [fig.channel[kind], fig.other[kind]]
    elif kind in (CPA, CPH) and not is_total:
        pools.append(others.get(kind, ()))  # a cost benchmark is a market fact
        if fig is not None:
            pools.append(fig.other[kind])
    core = cell.strip().strip("*_ ").strip()
    if core and _PURE_VALUE_RE.match(core):
        ends = _endpoints(core)
        if not ends or _all_in(ends, *pools):
            return cell, 0
        at = cell.find(core) + len(core)
        return cell[:at] + DAGGER + cell[at:], 1
    marks = 0
    pieces: List[str] = []
    pos = 0
    for m in _TOKEN_RE.finditer(cell):
        if re.match(r"\s*%", cell[m.end() : m.end() + 2]):
            continue  # a percentage, not a count
        ends = _endpoints(m.group(0))
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
    return re.sub(_DASH, "-", s)


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
    user_turns: Optional[Sequence[str]] = None,
) -> Tuple[str, Dict[str, int]]:
    """Hold the answer's plan figures to the engine's by MARKING.

    * a hires / applications / CPA / cost-per-hire figure that is the engine's
      range, a channel's own, a number the user typed in ANY turn, or another
      tool's cost benchmark is left alone;
    * a figure that traces to none of them gets a dagger, in prose and in tables
      laid out either way; no text is ever rewritten (``rewritten`` is always 0,
      kept for callers);
    * a plan answer that carries no engine range gets the engine's block, and a
      plan answer that quotes figures gets one basis / best-case line.

    The guard runs when the turn has a projection, when the caller says the
    answer is a plan (``plan_context``), or when any user turn names a budget.
    ``user_turns`` are the user's messages of the whole conversation (the
    current one is ``user_message``). Returns ``(text, {"rewritten", "marked",
    "appended", "claims"})``. Never raises; its work is capped per answer.

    Known limits are listed above the claim patterns (channel words, rates
    without the plan in the clause, answers in another language). Past
    ``MAX_CHECKS_PER_ANSWER`` checks the answer gains ``CAP_NOTE`` once.
    """
    stats = {"rewritten": 0, "marked": 0, "appended": 0, "claims": 0}
    if not response_text or not isinstance(response_text, str):
        return response_text, stats
    turns = [t for t in (user_turns or []) if isinstance(t, str)]
    if user_message and (not turns or turns[-1] != user_message):
        turns.append(user_message)
    try:
        return _enforce(
            response_text, tool_results, turns, plan_context, plan_shaped, stats
        )
    except Exception as exc:  # noqa: BLE001 - the guard must never block an answer
        logger.warning(
            "nova_plan_figures: guard failed (non-fatal): %s", exc, exc_info=True
        )
        return response_text, {"rewritten": 0, "marked": 0, "appended": 0, "claims": 0}


_PROJECTED_KEY_RE = re.compile(r"project|predict", re.IGNORECASE)


def _enforce(
    text: str,
    tool_results: Sequence[Any],
    user_turns: Sequence[str],
    plan_context: bool,
    plan_shaped: bool,
    stats: Dict[str, int],
) -> Tuple[str, Dict[str, int]]:
    if CAP_NOTE in text:
        return text, stats  # already checked as far as the cap allows
    engines, other_results = collect(tool_results)
    if (
        not engines
        and not plan_context
        and not _conversation_names_a_budget(user_turns)
    ):
        return text, stats

    user = _user_pools(user_turns)
    # another tool's figures excuse a CPA / CPH as a market benchmark; its OWN
    # projected outcomes ("total_projected", "predicted_...") are a second
    # calculator's plan totals, not a fact about the market
    others: Dict[str, Set[float]] = {k: set() for k in KINDS}
    for res in other_results:
        _walk_kind_numbers(
            {k: v for k, v in res.items() if not _PROJECTED_KEY_RE.search(str(k))},
            others,
        )
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
        if one is not None:
            merged.budget = one.budget
    months = _campaign_months(engines, user_turns)

    # ---- prose: MARK ONLY ----------------------------------------------------
    protect = _protected_spans(text)
    index = _Index(text, [f.budget for f in engines if f.budget])
    marks_at: List[int] = []
    quoted_kinds: Set[str] = set()
    checks = 0
    capped = False
    for claim in _claims(text):
        if _inside(claim.vs, protect):
            continue  # code, quotes, links: never checked, never counted
        checks += 1
        if checks > MAX_CHECKS_PER_ANSWER:
            capped = True
            break
        if text[claim.ve : claim.ve + 1] == DAGGER:
            continue  # already marked: a second pass adds nothing
        sa, sb = index.sentence(claim.vs, claim.ve)
        if text[sa:sb].rstrip().endswith("?"):
            continue  # a question to the user asserts no figure
        ends = _endpoints(text[claim.vs : claim.ve])
        if not ends:
            continue
        ca, cb, wide = index.clause(claim.vs, claim.ve)
        cues = index.cues(ca, cb, wide)
        rate = _rate_period(text, claim)
        if _not_a_plan_claim(text, claim, ends, cues, rate):
            continue
        kind = claim.kind
        if (
            kind == APPS
            and re.match(r"\s*apps\b", text[claim.ve : claim.me], re.IGNORECASE)
            and not (cues["plan"] or cues["expect"])
        ):
            continue  # "3 apps": mobile apps, unless the clause is about the plan
        if (cues["third"] or cues["bench"]) and not cues["plan"]:
            continue  # an agency, a competitor, the market, the past
        if (rate or _FOREIGN_CUR_RE.search(text[claim.vs : claim.ve])) and not cues[
            "plan"
        ]:
            # a rate, or an amount in another currency, is a statement about the
            # plan only where the clause names the plan ("In Japan, CPA averages
            # ¥2,800" is about a market)
            continue
        stats["claims"] += 1
        if _all_in(ends, user[kind]):
            continue  # the user's own figure, from any turn of the conversation
        quoted_kinds.add(kind)
        if rate:
            if _rate_consistent(ends, kind, rate, engines, months):
                continue
        elif merged is not None and _all_in(ends, merged.plan[kind]):
            continue  # the engine's own figure
        else:
            # a cost benchmark from another tool is a fact about the market; a
            # channel's own figure is true of that channel
            benign: List[Iterable[float]] = []
            if kind in (CPA, CPH) or cues["channel"]:
                benign.append(others.get(kind, ()))
            if merged is not None and cues["channel"]:
                benign.append(merged.channel[kind])
            if benign and _all_in(ends, *benign):
                continue
        if text[claim.me : claim.me + 1] == DAGGER:
            stats["marked"] += 1  # the answer already marks it ("900 applications†")
            continue
        marks_at.append(claim.ve)

    # The engine planned the budget as a campaign TOTAL; an answer that calls
    # that same amount a monthly (quarterly, yearly) budget misstates the basis.
    if merged is not None and merged.budget and one is not None:
        for pm in _PERIOD_BUDGET_RE.finditer(text):
            if _inside(pm.start(), protect) or text[pm.end() : pm.end() + 1] == DAGGER:
                continue
            ends = _endpoints(pm.group("amt"))
            if not ends or re.search(r"\b1[- ]month\b", one.reading or ""):
                continue
            amount = ends[0][0] * ends[0][2]
            if abs(amount - merged.budget) <= 0.01 * merged.budget:
                marks_at.append(pm.end())

    out = text
    for pos in sorted(set(marks_at), reverse=True):
        out = out[:pos] + DAGGER + out[pos:]
    stats["marked"] += len(set(marks_at))

    # ---- tables: mark only, total rows first --------------------------------
    out, table_marks, table_capped = _mark_table_cells(
        out, merged, user, others, MAX_CHECKS_PER_ANSWER - checks
    )
    stats["marked"] += table_marks
    if (capped or table_capped) and CAP_NOTE not in out:
        out = out.rstrip() + "\n\n" + CAP_NOTE

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
    return out, stats
