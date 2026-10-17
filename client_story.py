"""client_story.py -- the plain-language "Client story" for a generated plan.

Sales reps paste an exported plan into an AI chat "to tell the story of what the
client will get". This module builds that story in the product instead, from the
plan's OWN computed fields and nothing else:

* markets, roles, funded channels (+ the engine's grounded reason for each),
  budget and campaign window come straight off the plan dict;
* every projected figure (applications, cost per application, hires, cost per
  hire) is read from ``plan_projection.projection_for_alloc`` -- the one
  projection the workbook, deck, wizard and Nova already share -- so the story
  can never quote a number the workbook does not;
* pacing months come from the plan's activation calendar;
* a market with no local cost-per-hire benchmark gets the standard "not
  projected" sentence and no hire or cost-per-hire figure anywhere in the text.

Deterministic template composition: no LLM call, no randomness, no network, no
new arithmetic on plan numbers (counts of list items and percentage text are the
only things formatted). Thread-safe, never raises: on any failure the result is
``{"ok": False, ...}`` and callers simply omit the story.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import plan_projection

logger = logging.getLogger(__name__)

TITLE = "Client story"
# How many items each list names before collapsing to "and N more".
_MAX_MARKETS = 3
_MAX_ROLES = 3
_MAX_CHANNELS = 3
_MAX_PEAK_MONTHS = 3

_ESTIMATE_CAVEAT = (
    "These are modelled estimates built from benchmarks, not commitments."
)
# Words a client-facing summary must never contain (also enforced by tests).
FORBIDDEN_PHRASES = (
    "guaranteed",
    "guarantee",
    "best",
    "#1",
    "number one",
    "will hire",
    "industry-leading",
    "unmatched",
    "proven",
)


def _join(items: List[str]) -> str:
    items = [i for i in items if i]
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    return ", ".join(items[:-1]) + " and " + items[-1]


def _capped(items: List[str], limit: int, sep: str = ", ") -> str:
    """ "A, B and C" -- or "A, B, C (and 2 more)" past ``limit``. ``sep="; "``
    is for items that contain commas themselves ("Dallas, TX")."""
    items = [i for i in items if i]
    if sep == ", " and len(items) <= limit:
        return _join(items)
    shown = sep.join(items[:limit])
    return shown + (f" (and {len(items) - limit} more)" if len(items) > limit else "")


def _markets(data: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for loc in data.get("locations") or []:
        if isinstance(loc, dict):
            text = ", ".join(
                str(loc.get(k) or "").strip()
                for k in ("city", "state", "country")
                if loc.get(k)
            )
        else:
            text = str(loc or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _roles(data: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for role in data.get("target_roles") or data.get("roles") or []:
        title = (
            str(role.get("title") or role.get("role") or "")
            if isinstance(role, dict)
            else str(role or "")
        ).strip()
        if title and title not in out:
            out.append(title)
    return out


def _client_name(data: Dict[str, Any]) -> str:
    raw = str(data.get("client_name") or data.get("company_name") or "").strip()
    try:
        import display_format

        return display_format.client_display_name(raw) or raw or "the client"
    except (ImportError, AttributeError, TypeError, ValueError):
        return raw or "the client"


def _channel_label(key: str) -> str:
    try:
        import display_format

        return display_format.channel_label(key)
    except (ImportError, AttributeError):
        return key.replace("_", " ").title()


def _funded_channels(alloc: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Channels with budget, largest first: {key, label, pct, why}."""
    chans = alloc.get("channel_allocations")
    if not isinstance(chans, dict):
        return []
    try:
        import budget_engine

        avg = budget_engine.plan_average_cpa(chans)
        why_fn = budget_engine.channel_why
    except (ImportError, AttributeError):
        avg, why_fn = None, None
    rows = []
    for key, entry in chans.items():
        if not isinstance(entry, dict):
            continue
        try:
            amount = float(entry.get("dollar_amount") or 0)
        except (TypeError, ValueError):
            continue
        if amount <= 0:
            continue
        why = str(entry.get("why") or "")
        if not why and why_fn is not None:
            why = why_fn(entry, avg)
        pct = entry.get("percentage")
        rows.append(
            {
                "key": str(key),
                "label": _channel_label(str(key)),
                "amount": amount,
                "pct": (
                    pct
                    if isinstance(pct, (int, float)) and not isinstance(pct, bool)
                    else None
                ),
                "why": why.strip().rstrip("."),
            }
        )
    rows.sort(key=lambda r: -r["amount"])
    return rows


def _projection(
    data: Dict[str, Any], alloc: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    meta = alloc.get("metadata") if isinstance(alloc.get("metadata"), dict) else {}
    budget = meta.get("total_budget")
    if not isinstance(budget, (int, float)) or budget <= 0:
        return None
    # the plan's currency from the ONE shared resolver the deck, workbook and
    # delivery gate use (a plan the engine priced in USD but that displays in
    # another currency is parity arithmetic there, and here too)
    code, symbol = None, None
    try:
        import plan_currency

        code = plan_currency.currency_for_plan(data)
        symbol = plan_currency.symbol_for_code(code)
    except (ImportError, AttributeError, TypeError, ValueError):
        logger.debug("client story: plan currency unresolved", exc_info=True)
    proj = plan_projection.projection_for_alloc(
        alloc,
        budget=float(budget),
        industry_key=str(meta.get("industry") or ""),
        role_titles=[str(r) for r in (alloc.get("role_allocations") or {})],
        symbol=symbol,
        display_currency=code,
        non_us_market=plan_projection.is_non_us_market(data.get("locations")),
        mixed_market=plan_projection.is_mixed_market(data.get("locations")),
    )
    return proj if isinstance(proj, dict) and proj.get("ok") else None


def _pacing_months(data: Dict[str, Any]) -> Dict[str, Any]:
    gold = (
        data.get("_gold_standard")
        if isinstance(data.get("_gold_standard"), dict)
        else {}
    )
    cal = (
        gold.get("activation_calendar")
        if isinstance(gold.get("activation_calendar"), dict)
        else {}
    )
    timeline = cal.get("timeline") if isinstance(cal.get("timeline"), list) else []
    rows = [m for m in timeline if isinstance(m, dict) and m.get("month_name")]

    def weight(m: Dict[str, Any]) -> float:
        w = m.get("budget_weight")
        return (
            float(w) if isinstance(w, (int, float)) and not isinstance(w, bool) else 0.0
        )

    peaks = sorted(
        (m for m in rows if str(m.get("hiring_intensity") or "").lower() == "high"),
        key=lambda m: -weight(m),
    )
    lows = [m for m in rows if str(m.get("hiring_intensity") or "").lower() == "low"]
    start = next((m for m in rows if m.get("offset_from_start") == 0), None)
    return {
        "start": str(start["month_name"]) if start else "",
        "peaks": [str(m["month_name"]) for m in peaks[:_MAX_PEAK_MONTHS]],
        "lows": [str(m["month_name"]) for m in lows],
    }


def _duration_label(data: Dict[str, Any]) -> str:
    try:
        import display_format

        label = display_format.resolve_campaign_duration_label(data)
    except (ImportError, AttributeError, TypeError, ValueError):
        label = str(data.get("campaign_duration") or "")
    return "" if label.strip().lower() == "not specified" else label.strip()


def _what_we_will_do(data, alloc, proj, channels) -> str:
    symbol = (proj.get("currency") or {}).get("symbol") or "$"
    budget = f"{symbol}{proj['budget']:,.0f}"
    markets = _capped(_markets(data), _MAX_MARKETS, "; ")
    roles = _capped(_roles(data), _MAX_ROLES)
    lead = f"For {_client_name(data)}, this plan puts {budget} to work"
    if roles:
        lead += f" recruiting {roles}"
    if markets:
        lead += f" in {markets}"
    lead += (
        f", across {len(channels)} funded channel{'s' if len(channels) != 1 else ''}."
    )
    parts = [lead]
    for i, ch in enumerate(channels[:_MAX_CHANNELS]):
        share = f", {ch['pct']:g}% of budget" if ch["pct"] is not None else ""
        why = f" ({ch['why']})" if ch["why"] else ""
        lead_in = "Largest channel: " if i == 0 else "Next: "
        parts.append(f"{lead_in}{ch['label']}{share}{why}.")
    return " ".join(parts)


def _what_to_expect(proj: Dict[str, Any]) -> str:
    txt = proj.get("text") or {}
    a = proj.get("assumptions") or {}
    parts = []
    if txt.get("applications") and txt["applications"] != "n/a":
        sent = f"At this budget the model estimates {txt['applications']} applications"
        if txt.get("cpa") and txt["cpa"] != "n/a":
            sent += f" at {txt['cpa']} per application"
        parts.append(sent + ".")
    if a.get("hires_suppressed"):
        parts.append(txt.get("hires_note") or plan_projection.NO_LOCAL_HIRES_NOTE)
    elif txt.get("hires") and txt["hires"] != "n/a" and not a.get("below_one_hire"):
        sent = f"Estimated hires: {txt['hires']}"
        if txt.get("cph") and txt["cph"] != "n/a":
            sent += f" at {txt['cph']} per hire"
        parts.append(sent + ".")
        parts.append(txt.get("hires_short") or "")
    else:
        parts.append(txt.get("hires_short") or "")
    parts.append(_ESTIMATE_CAVEAT)
    return " ".join(p for p in parts if p)


def _how_spend_is_paced(data: Dict[str, Any]) -> str:
    pace = _pacing_months(data)
    duration = _duration_label(data)
    parts = []
    window = (
        f"The campaign runs for {duration}"
        if duration
        else "The campaign window follows the brief"
    )
    if pace["start"]:
        window += f", starting in {pace['start']}"
    parts.append(window + ".")
    if pace["peaks"]:
        sent = f"Spend is weighted toward {_join(pace['peaks'])}, the plan's higher-intensity hiring months"
        if pace["lows"]:
            sent += f", and held lighter in {_join(pace['lows'])}"
        parts.append(sent + ".")
    return " ".join(parts)


def _what_we_need(data: Dict[str, Any], alloc: Dict[str, Any]) -> str:
    asks: List[str] = []
    sor = str(data.get("enterprise_system_of_record") or "").strip()
    if sor:
        asks.append(f"Please confirm {sor} as the system of record for hires.")
    else:
        asks.append(
            "Please name the system of record (for example your applicant tracking "
            "system) that will confirm hires, so results can be reported on hires "
            "and not only applications."
        )
    asks.append("Please approve the roles, markets and job-ad content before launch.")
    excluded = data.get("excluded_publishers")
    if isinstance(excluded, list) and excluded:
        n = len(excluded)
        asks.append(
            f"Please confirm your exclusion list ({n} board{'s' if n != 1 else ''}); "
            f"{'they are' if n != 1 else 'it is'} left out of the plan."
        )
    meta = alloc.get("metadata") if isinstance(alloc.get("metadata"), dict) else {}
    gated = (meta.get("vendor_gate") or {}).get("gated_channels")
    if isinstance(gated, list) and gated:
        n = len(gated)
        asks.append(
            f"{n} channel{'s' if n != 1 else ''} not available for this plan's markets "
            f"{'are' if n != 1 else 'is'} left out."
        )
    lock = str(data.get("enterprise_request_lock_day") or "").strip()
    sla = data.get("enterprise_setup_sla_days")
    if lock:
        asks.append(f"Please agree the weekly request lock day ({lock.title()}).")
    if isinstance(sla, int) and not isinstance(sla, bool) and sla > 0:
        asks.append(f"Please confirm the {sla}-day setup window.")
    return " ".join(asks)


_NEXT_STEPS = (
    "Review and approve this plan, share tracking access, and we confirm the launch "
    "date; results are reviewed against these estimates as data comes in."
)


def build_client_story(plan_data: Any) -> Dict[str, Any]:
    """The client story for a generated plan.

    Returns ``{"ok": True, "title", "sections": [{"key", "heading", "text"}],
    "text", "word_count"}`` or ``{"ok": False, "reason"}`` when the plan has no
    funded channels / projection. ``text`` is the copy-friendly block (headings
    plus paragraphs). Never raises."""
    try:
        if not isinstance(plan_data, dict):
            return {"ok": False, "reason": "no plan"}
        alloc = plan_data.get("_budget_allocation")
        if not isinstance(alloc, dict) or not alloc:
            return {"ok": False, "reason": "no budget allocation"}
        channels = _funded_channels(alloc)
        proj = _projection(plan_data, alloc)
        if not channels or proj is None:
            return {"ok": False, "reason": "no funded channels or projection"}
        sections = [
            (
                "what_we_will_do",
                "What we will do",
                _what_we_will_do(plan_data, alloc, proj, channels),
            ),
            ("what_to_expect", "What to expect", _what_to_expect(proj)),
            ("pacing", "How spend is paced", _how_spend_is_paced(plan_data)),
            ("needs", "What we need from you", _what_we_need(plan_data, alloc)),
            ("next_steps", "Next steps", _NEXT_STEPS),
        ]
        rows = [{"key": k, "heading": h, "text": t} for k, h, t in sections if t]
        text = "\n\n".join(f"{r['heading']}\n{r['text']}" for r in rows)
        return {
            "ok": True,
            "title": TITLE,
            "sections": rows,
            "text": text,
            "word_count": len(text.split()),
        }
    except (
        Exception
    ) as exc:  # noqa: BLE001 - the story is best-effort, never blocks a plan
        logger.error("client story failed (non-fatal): %s", exc, exc_info=True)
        return {"ok": False, "reason": "error"}
