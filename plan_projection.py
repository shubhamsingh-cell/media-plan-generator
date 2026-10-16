"""plan_projection.py -- ONE projection engine front door.

Every surface that shows "applications / hires / cost per application / cost
per hire" for a plan (the wizard's live estimate, the Quick Plan page, Nova's
projection tool, the workbook, the deck, the MCP server) reads the dict this
module builds, so a $60k brief can no longer read "19 hires / $3,158" in the
wizard, "245 hires / $245" on Quick Plan and "35 hires" in chat.

Why a module of its own: ``budget_engine.calculate_budget_allocation`` stays
the single calculator, but its ``total_projected`` is a set of POINT figures
(the hires range exists, the others do not) and every consumer used to rebuild
the rest -- ranges, CPA realism, the "what does this assume" label -- on its
own. This is the one place that does it.

How each range is derived (nothing here is an invented +/- percentage):

* **hires** -- the engine's own ``hires_low`` / ``hires_high``: budget divided by
  the industry-average cost per hire (low) and by the plan-efficiency floor,
  0.5 x that average (high). They are an ASSUMPTION, not a forecast: no client
  hire history is an input, so the label says so and names the conversion basis.
* **cost per hire** -- budget / hires, so it is the same range inverted.
* **cost per application** -- low is the engine's figure (budget / modelled
  applications, a CPC x apply-rate calculation with no CPA floor). When the
  repo's industry benchmark (``data/recruitment_benchmarks_deep.json``
  ``industry_benchmarks.<industry>.cpa.range``, or its sub-role row such as
  ``logistics_supply_chain.cpa.warehouse_hourly``) runs HIGHER than the engine,
  the high end widens to that benchmark's low end and a note says so. The
  engine figure is never clamped.
* **applications** -- budget / the CPA range, so the three figures always
  multiply back to the budget at both ends.

Thread-safe (no module state beyond a read-only cache), never raises: on any
failure ``project_plan`` returns ``{"ok": False, "error": ...}``.
"""

from __future__ import annotations

import logging
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Engine legacy industry key -> data/recruitment_benchmarks_deep.json
# ``industry_benchmarks`` key. The standardizer's ``deep_bench_key`` is the
# general mapping; these are the legacy keys it routes to a DIFFERENT sector
# ("blue_collar_trades" -> logistics, "education" -> government,
# "general_entry_level" -> retail) where the KB has a better-named row.
_ENGINE_TO_KB_INDUSTRY: Dict[str, str] = {
    "blue_collar_trades": "blue_collar_skilled_trades",
    "education": "education",
    "automotive": "automotive_manufacturing",
    "general_entry_level": "retail_consumer",
}

# Words in a KB cpa sub-key that describe the pay band / difficulty rather than
# the role ("warehouse_hourly", "entry_level", "senior_specialized"); a sub-key
# applies to a brief only when every REMAINING word is in a role title.
_SUBKEY_GENERIC = frozenset(
    {
        "hourly", "roles", "role", "level", "entry", "senior", "specialized",
        "easy", "hard", "to", "fill", "competitive", "markets", "market",
        "ai", "ml", "management", "median", "range", "trend", "yoy", "notes",
    }
)

_FALLBACK_INDUSTRY = "general_entry_level"

# Default channel mix when ppt_generator.INDUSTRY_ALLOC_PROFILES has no row --
# the same fallback the wizard's /api/estimate uses.
_DEFAULT_ALLOC = {
    "programmatic_dsp": 35,
    "global_boards": 20,
    "niche_boards": 15,
    "social_media": 12,
    "regional_boards": 8,
    "employer_branding": 5,
    "apac_regional": 3,
    "emea_regional": 2,
}

_KB_CPA_SOURCE_FILE = "data/recruitment_benchmarks_deep.json"

# The one sentence every surface prints where hires / cost per hire are not
# projected (a market with no local cost-per-hire benchmark in its own currency).
NO_LOCAL_HIRES_NOTE = (
    "Hires: no local cost-per-hire benchmark for this market \u2014 not projected."
)


MIXED_MARKETS_LABEL = "basis: US benchmark; non-US markets not separately projected"


def is_non_us_market(locations: Any) -> bool:
    """True when NO market in the plan is a US market (location-only: the same
    test every surface applies, whatever the wizard's region default). A plan
    with at least one US market has a cost-per-hire benchmark to stand on."""
    try:
        import plan_geo

        return not plan_geo.plan_has_us_market({"locations": list(locations or [])})
    except Exception as exc:  # noqa: BLE001 - classification is best-effort
        logger.debug("plan_projection: market check unavailable: %s", exc)
        return False


def is_mixed_market(locations: Any) -> bool:
    """True when the plan has at least one US market AND at least one non-US one."""
    try:
        import plan_geo

        locs = {"locations": list(locations or [])}
        return plan_geo.plan_has_us_market(locs) and not plan_geo.is_us_plan(locs)
    except Exception as exc:  # noqa: BLE001
        logger.debug("plan_projection: market check unavailable: %s", exc)
        return False


# --------------------------------------------------------------------------
# small parsing helpers
# --------------------------------------------------------------------------


def _num(val: Any) -> Optional[float]:
    if isinstance(val, bool) or not isinstance(val, (int, float)):
        return None
    return float(val)


def _parse_dollar_range(text: Any) -> Optional[Tuple[float, float]]:
    """"$35-$58+" -> (35, 58); "$12" -> (12, 12); anything else -> None."""
    if not isinstance(text, str):
        return None
    nums: List[float] = []
    for amt in re.findall(r"\$\s*([\d,]+(?:\.\d+)?)", text):
        try:
            nums.append(float(amt.replace(",", "")))
        except ValueError:
            continue
    if not nums:
        return None
    low, high = nums[0], nums[1] if len(nums) > 1 else nums[0]
    if high < low:
        low, high = high, low
    return low, high


def _stem(word: str) -> str:
    word = word.lower()
    return word[:-1] if len(word) > 3 and word.endswith("s") else word


def split_location(loc: Any) -> Dict[str, str]:
    """Free-form location -> {"city", "state", "country"}.

    Same rules as app._split_city_state_country (a 2-letter US state or a US
    state name is a STATE, never the country; "City, Country" is a country).
    Dicts pass through. Parity-tested against the app helper.
    """
    if isinstance(loc, dict):
        return {
            "city": str(loc.get("city") or ""),
            "state": str(loc.get("state") or ""),
            "country": str(loc.get("country") or ""),
        }
    text = str(loc or "")
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if not parts:
        return {"city": text, "state": "", "country": "United States"}
    if len(parts) >= 3:
        return {"city": parts[0], "state": parts[1], "country": parts[2]}
    if len(parts) == 2:
        second = parts[1]
        is_state = False
        try:
            import plan_geo

            is_state = second.lower() in plan_geo.US_STATE_NAME_TO_ABBR or (
                len(second) == 2
                and second.isalpha()
                and second.upper() in plan_geo.US_STATE_ABBR
            )
        except Exception:  # noqa: BLE001 - classification is best-effort
            is_state = False
        if is_state:
            return {"city": parts[0], "state": second, "country": "United States"}
        return {"city": parts[0], "state": "", "country": second}
    return {"city": parts[0], "state": "", "country": "United States"}


# --------------------------------------------------------------------------
# knowledge-base lookups (all read-only, all cited by file + key)
# --------------------------------------------------------------------------


def _load_kb(knowledge_base: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if isinstance(knowledge_base, dict) and knowledge_base:
        return knowledge_base
    try:
        from kb_loader import load_knowledge_base

        return load_knowledge_base() or {}
    except Exception as exc:  # noqa: BLE001 - KB is optional context
        logger.debug("plan_projection: knowledge base unavailable: %s", exc)
        return {}


def kb_industry_key(industry_key: str) -> str:
    """Engine industry key -> the KB ``industry_benchmarks`` key ("" if none)."""
    key = str(industry_key or "").strip()
    if not key:
        return ""
    if key in _ENGINE_TO_KB_INDUSTRY:
        return _ENGINE_TO_KB_INDUSTRY[key]
    try:
        import standardizer

        canonical = standardizer.normalize_industry(key)
        meta = standardizer.CANONICAL_INDUSTRIES.get(canonical) or {}
        return str(meta.get("deep_bench_key") or "")
    except Exception as exc:  # noqa: BLE001
        logger.debug("plan_projection: standardizer lookup failed: %s", exc)
        return ""


def _kb_industry_row(
    industry_key: str, knowledge_base: Optional[Dict[str, Any]]
) -> Tuple[str, Dict[str, Any]]:
    kb = _load_kb(knowledge_base)
    rb = kb.get("recruitment_benchmarks")
    table = rb.get("industry_benchmarks") if isinstance(rb, dict) else None
    if not isinstance(table, dict):
        return "", {}
    kb_key = industry_key if industry_key in table else kb_industry_key(industry_key)
    row = table.get(kb_key)
    return (kb_key, row) if isinstance(row, dict) else ("", {})


def kb_cpa_range(
    industry_key: str,
    role_titles: Optional[List[str]] = None,
    knowledge_base: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """The KB's cost-per-application range for this industry / role, or None.

    A sub-role row (``cpa.warehouse_hourly``) wins over the industry row
    (``cpa.range``) when the brief's role titles contain its words. Returns
    ``{"low", "high", "text", "scope", "key", "source"}``.
    """
    kb_key, row = _kb_industry_row(industry_key, knowledge_base)
    cpa = row.get("cpa") if row else None
    if not isinstance(cpa, dict):
        return None
    role_words = {
        _stem(w)
        for title in (role_titles or [])
        for w in re.findall(r"[A-Za-z]+", str(title or ""))
    }
    for sub_key, text in cpa.items():
        if sub_key in ("range", "median") or not isinstance(text, str):
            continue
        words = [
            w for w in sub_key.lower().split("_") if w and w not in _SUBKEY_GENERIC
        ]
        if not words or not all(_stem(w) in role_words for w in words):
            continue
        rng = _parse_dollar_range(text)
        if rng:
            return {
                "low": rng[0],
                "high": rng[1],
                "text": text,
                "scope": "sub_role",
                "key": f"{kb_key}.cpa.{sub_key}",
                "source": f"{_KB_CPA_SOURCE_FILE} industry_benchmarks.{kb_key}.cpa.{sub_key}",
            }
    rng = _parse_dollar_range(cpa.get("range"))
    if not rng:
        return None
    return {
        "low": rng[0],
        "high": rng[1],
        "text": str(cpa.get("range")),
        "scope": "industry",
        "key": f"{kb_key}.cpa.range",
        "source": f"{_KB_CPA_SOURCE_FILE} industry_benchmarks.{kb_key}.cpa.range",
    }


def kb_hire_conversion(
    industry_key: str, knowledge_base: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """KB conversion references for the hires label: media-only cost per hire
    (``cph.recruitment_marketing_only``), total cost per hire
    (``cph.total_cost_per_hire``) and ``applicants_per_hire`` -- each only when
    the KB carries it."""
    kb_key, row = _kb_industry_row(industry_key, knowledge_base)
    out: Dict[str, Any] = {}
    cph = row.get("cph") if row else None
    if isinstance(cph, dict):
        media = cph.get("recruitment_marketing_only")
        rng = _parse_dollar_range(media)
        if rng:
            out["media_only_cph"] = {
                "low": rng[0],
                "high": rng[1],
                "text": str(media),
                "source": f"{_KB_CPA_SOURCE_FILE} industry_benchmarks.{kb_key}.cph.recruitment_marketing_only",
            }
    aph = _num(row.get("applicants_per_hire")) if row else None
    if aph and aph > 0:
        out["applicants_per_hire"] = {
            "value": aph,
            "source": f"{_KB_CPA_SOURCE_FILE} industry_benchmarks.{kb_key}.applicants_per_hire",
        }
    return out


# --------------------------------------------------------------------------
# range assembly
# --------------------------------------------------------------------------


def _cpa_range(
    engine_cpa: float, kb: Optional[Dict[str, Any]]
) -> Tuple[float, float, Optional[str]]:
    """(low, high, note) for cost per application -- see the module docstring.

    The range is the HULL of the engine figure and the knowledge base's band:
    ``[min(engine, kb_low), max(engine, kb_high)]``. That makes it continuous
    and monotone in the engine figure (a $14.77 engine CPA against a $14.78
    benchmark low no longer yields a $0.01-wide range while $14.79 yields
    $14.79-$89), never narrower than the KB band, and it keeps the engine
    figure as the low end whenever the engine is cheaper than the benchmark.
    ``note`` is the cited form (file + key)."""
    if not kb or engine_cpa <= 0:
        return engine_cpa, engine_cpa, None
    lo, hi = float(kb["low"]), float(kb["high"])
    where = "sub-role" if kb.get("scope") == "sub_role" else "industry"
    band = f"{_money(lo)}\u2013{_money(hi)}"
    cite = f"({where}; source: {kb['source']})"
    low, high = min(engine_cpa, lo), max(engine_cpa, hi)
    if engine_cpa < lo:
        note = (
            f"Industry benchmarks run higher: {band} per application {cite}. "
            f"The modelled {_money(engine_cpa)} is the optimistic end."
        )
    elif engine_cpa <= hi:
        note = (
            f"Industry benchmark range {band} per application {cite}; "
            f"the modelled {_money(engine_cpa)} sits inside it."
        )
    else:
        note = f"Industry benchmarks run lower: {band} per application {cite}."
    return low, high, note


def _symbol_for(code: str) -> str:
    if code == "USD":
        return "$"
    try:
        import plan_currency

        return plan_currency.symbol_for_code(code) or f"{code} "
    except Exception as exc:  # noqa: BLE001 - presentation only
        logger.debug("plan_projection: currency symbol unavailable: %s", exc)
        return f"{code} "


def _money(value: float, symbol: str = "$") -> str:
    if value >= 100 or abs(value - round(value)) < 0.005:
        return f"{symbol}{value:,.0f}"
    return f"{symbol}{value:,.2f}"


def _int_range(low: Optional[float], high: Optional[float], mid: Optional[float]) -> Dict[str, Any]:
    if low is None or high is None:
        return {"low": None, "mid": None, "high": None}
    lo, hi = int(round(min(low, high))), int(round(max(low, high)))
    m = int(round(mid)) if mid is not None else (lo + hi) // 2
    return {"low": lo, "mid": min(max(m, lo), hi), "high": hi}


def _float_range(low: Optional[float], high: Optional[float], mid: Optional[float]) -> Dict[str, Any]:
    if low is None or high is None:
        return {"low": None, "mid": None, "high": None}
    lo, hi = round(min(low, high), 2), round(max(low, high), 2)
    m = round(mid, 2) if mid is not None else round((lo + hi) / 2, 2)
    return {"low": lo, "mid": min(max(m, lo), hi), "high": hi}


def ranges_from_engine_result(
    budget_result: Dict[str, Any],
    *,
    budget: float,
    industry_key: str,
    role_titles: Optional[List[str]] = None,
    knowledge_base: Optional[Dict[str, Any]] = None,
    symbol: Optional[str] = None,
    display_currency: Optional[str] = None,
    non_us_market: bool = False,
    mixed_market: bool = False,
) -> Dict[str, Any]:
    """Build the projection dict from an existing ``calculate_budget_allocation``
    result. Pure; used by ``project_plan`` and by callers that already hold an
    engine result (the wizard estimate, the workbook, the deck)."""
    tp = budget_result.get("total_projected") if isinstance(budget_result, dict) else None
    tp = tp if isinstance(tp, dict) else {}
    meta = budget_result.get("metadata") if isinstance(budget_result, dict) else None
    meta = meta if isinstance(meta, dict) else {}
    _icph = meta.get("industry_cph") if isinstance(meta.get("industry_cph"), dict) else {}
    plan_ccy = str(_icph.get("plan_currency") or "USD").upper()
    engine_ccy = plan_ccy
    # A plan the engine priced in one currency but the deliverable DISPLAYS in
    # another (the engine ran without a plan currency) is parity arithmetic:
    # treat it as the display currency, whose benchmarks it never had.
    if display_currency and str(display_currency).upper() != plan_ccy:
        plan_ccy = str(display_currency).upper()
    if symbol is None:
        symbol = _symbol_for(plan_ccy)

    apps_engine = int(tp.get("applications") or 0)
    hires_engine = int(tp.get("hires") or 0)
    cph_engine = _num(tp.get("cost_per_hire")) or 0.0
    cpa_engine = (budget / apps_engine) if apps_engine > 0 else 0.0

    notes: List[str] = []

    # -- cost per application / applications --------------------------------
    # The KB's cost-per-application ranges are US$: they are only comparable
    # with a USD plan (never an FX guess against a local-currency CPA).
    # ... and only meaningful where the market IS the US benchmark's market: a
    # plan with no US market (even one priced in dollars) gets no US band.
    kb_cpa = (
        kb_cpa_range(industry_key, role_titles, knowledge_base)
        if plan_ccy == "USD" and not non_us_market
        else None
    )
    cpa_lo, cpa_hi, cpa_note = _cpa_range(cpa_engine, kb_cpa)
    if plan_ccy != "USD" and cpa_engine > 0:
        notes.append(
            "Cost per application is a single estimate: the industry benchmark "
            f"range is in US$ and this plan is priced in {plan_ccy}."
        )
    elif non_us_market and cpa_engine > 0:
        notes.append(
            "Cost per application is a single estimate: the industry benchmark "
            "range is a US figure \u2014 no local data for this market."
        )
    if cpa_note:
        notes.append(cpa_note)
    if cpa_engine > 0:
        # The optimistic end IS the engine's own application count when the
        # CPA low end is the engine figure, so it is never re-rounded.
        apps_hi = apps_engine if cpa_lo == cpa_engine else budget / cpa_lo
        apps = _int_range(budget / cpa_hi, apps_hi, None)
        cpa = _float_range(cpa_lo, cpa_hi, None)
    else:
        apps = _int_range(0, 0, 0)
        cpa = _float_range(None, None, None)

    # -- hires / cost per hire ----------------------------------------------
    # Bounds come from the cost-per-hire BASIS the plan cites (the industry
    # average and the plan-efficiency floor = half of it): hires are budget /
    # basis (rounded down at both ends) and cost per hire IS the basis, so a
    # rounded hire count can never push the displayed cost per hire outside
    # the basis it states ($15k: 1-2 hires at $5,250-$10,500, not $7,500-$15,000).
    icph = meta.get("industry_cph") if isinstance(meta.get("industry_cph"), dict) else {}
    floor = _num(meta.get("cph_benchmark_floor"))
    math_avg = _num(icph.get("math_value")) or _num(icph.get("value")) or _num(meta.get("industry_avg_cph"))
    # an industry with no cost-per-hire range of its own shows the knowledge
    # base's row for it: the engine's hires range is anchored on THAT midpoint
    # (design-judge 2026-10-01 item 3), so the projection must be too
    _row_mid = _num((icph.get("display_row") or {}).get("mid")) if isinstance(icph.get("display_row"), dict) else None
    if _row_mid and _row_mid > 0:
        math_avg = _row_mid
    basis_ccy = str(icph.get("currency") or "USD").upper()
    suppressed = bool(icph.get("claim_suppressed"))
    # a USD benchmark under a non-USD plan with no exchange rate is parity
    # arithmetic, not a range (KSh 100,000 does not buy "7-16 hires at US$5,000")
    parity = basis_ccy != plan_ccy or engine_ccy != plan_ccy
    translated = suppressed and not parity  # US benchmark converted at a stated rate
    # The cost-per-hire basis is a US figure whenever the market is outside the
    # US and the basis is not a cited local range: say so on the figures.
    us_tag = translated or (non_us_market and str(icph.get("basis") or "") != "local_kb")
    fx = icph.get("fx") if isinstance(icph.get("fx"), dict) else {}
    basis_ok = bool(math_avg and math_avg > 0 and floor and floor > 0 and not parity)
    hires_is_range = False
    below_one_hire = False
    if basis_ok:
        h_lo = min(int(budget / math_avg), hires_engine)
        h_hi = max(int(budget / floor), hires_engine)
        if h_hi <= 0:
            below_one_hire = True
            hires = _int_range(0, 0, 0)
            cph = _float_range(None, None, None)
            notes.append(
                "Budget is below one hire at the industry cost per hire "
                f"({_money(floor, symbol)}-{_money(math_avg, symbol)}); no hires "
                "are projected and cost per hire is not defined."
            )
        else:
            hires = _int_range(h_lo, h_hi, hires_engine)
            cph = _float_range(
                min(floor, cph_engine or floor),
                max(math_avg, cph_engine or math_avg),
                cph_engine or None,
            )
            hires_is_range = hires["low"] != hires["high"]
    else:
        e_lo, e_hi = tp.get("hires_low"), tp.get("hires_high")
        if (
            not parity
            and isinstance(e_lo, (int, float))
            and isinstance(e_hi, (int, float))
            and e_hi > 0
        ):
            hires = _int_range(e_lo, e_hi, hires_engine)
            cph = _float_range(
                budget / max(hires["high"], 1), budget / max(hires["low"], 1), cph_engine or None
            )
            hires_is_range = hires["low"] != hires["high"]
        elif hires_engine <= 0:
            below_one_hire = True
            hires = _int_range(0, 0, 0)
            cph = _float_range(None, None, None)
            notes.append("Budget is below one hire at industry cost per hire; cost per hire is not defined.")
        else:
            hires = _int_range(hires_engine, hires_engine, hires_engine)
            cph = (
                _float_range(cph_engine, cph_engine, cph_engine)
                if cph_engine
                else _float_range(None, None, None)
            )
            if parity:
                notes.append(
                    f"Hires are a single estimate: this plan is priced in {plan_ccy} "
                    f"and the cost-per-hire benchmark is in {basis_ccy}, so no range "
                    "can be bounded without a local figure."
                )
            elif suppressed:
                notes.append(
                    "Hires are a single estimate: this market has no local "
                    "cost-per-hire benchmark."
                )
            else:
                notes.append("Hires are a single estimate: no cost-per-hire benchmark bounds a range.")
    if translated and basis_ok and not below_one_hire:
        rate = _num(fx.get("usd_per_local"))
        rate_txt = f"US$1 = {symbol}{1.0 / rate:,.2f}" if rate else "the engine's rate"
        when = f", {fx.get('source_short') or 'rate'} {fx['as_of']}" if fx.get("as_of") else ""
        notes.append(
            f"US benchmark — no local data: cost-per-hire basis is the US benchmark "
            f"converted at {rate_txt}{when}."
        )

    # -- no local cost-per-hire benchmark: hires and CPH are NOT projected -----
    # Declare, never convert: a market whose cost per hire is not benchmarked in
    # its own currency (the engine suppresses the claim; a USD benchmark under a
    # non-USD plan; a US-dollar plan in a non-US market with no local USD figure)
    # gets no hires or cost-per-hire figure on ANY surface -- applications and
    # cost per application still show, in the plan currency.
    hires_suppressed = bool(
        suppressed
        or parity
        or (non_us_market and str(icph.get("basis") or "") != "local_kb")
    )
    if hires_suppressed:
        hires = _int_range(None, None, None)
        cph = _float_range(None, None, None)
        hires_is_range = False
        below_one_hire = False
        translated = False
        us_tag = False
        basis_ok = False
        notes[:] = [n for n in notes if "hire" not in n.lower()]
        notes.append(NO_LOCAL_HIRES_NOTE)

    # -- assumptions (what the hires / CPH numbers rest on) ------------------
    avg = math_avg
    conv = kb_hire_conversion(industry_key, knowledge_base)
    if us_tag:
        basis_src = "the US cost-per-hire benchmark (US benchmark \u2014 no local data)"
    else:
        basis_src = "Nova's cost-per-hire table"
    assumptions: Dict[str, Any] = {
        "client_hire_history": False,
        "hires_basis": (
            f"Estimated, not forecast: budget divided by the industry cost per hire from {basis_src}"
            + (
                f" (low end at the industry average {_money(avg, symbol)}, high end at "
                f"the plan-efficiency floor {_money(floor, symbol)} = half that average)"
                if basis_ok
                else ""
            )
            + "; no client hire history was provided."
        ),
        "cph_concept": "total",
        "cph_basis": (
            "Cost per hire here is budget / estimated hires, compared against the "
            "industry's TOTAL cost per hire ("
            + (
                "the US benchmark \u2014 no local data"
                if us_tag
                else "Nova's INDUSTRY_CPH_RANGES table"
            )
            + "), not a media-only figure."
        ),
        "industry_cph_avg": avg if basis_ok else None,
        "industry_cph_floor": floor if basis_ok else None,
        "us_benchmark_translated": translated,
        "us_benchmark_no_local": us_tag,
        "below_one_hire": below_one_hire,
        "hires_suppressed": hires_suppressed,
        # a USD plan with a US market AND non-US ones: whole-plan hires on the
        # US basis, labelled (the engine does not project per market)
        "mixed_markets": bool(mixed_market and not hires_suppressed),
        "kb_cpa": kb_cpa,
        **conv,
    }
    media = conv.get("media_only_cph")
    if media:
        assumptions["cph_basis"] += (
            f" The knowledge base's media-only benchmark is {media['text']} "
            f"(US$; source: {media['source']})."
        )
    aph = conv.get("applicants_per_hire")
    if aph:
        assumptions["applicants_per_hire_note"] = (
            f"Knowledge-base reference: ~{aph['value']:,.0f} applicants per hire "
            f"(source: {aph['source']})."
        )

    return {
        "ok": True,
        "budget": float(budget),
        "currency": {"code": plan_ccy, "symbol": symbol},
        "industry": industry_key,
        "applications": apps,
        "hires": hires,
        "cpa": cpa,
        "cph": cph,
        "engine": {
            "applications": apps_engine,
            "hires": hires_engine,
            "cost_per_hire": cph_engine,
            "cost_per_application": round(cpa_engine, 2),
        },
        "is_range": {
            "applications": apps["low"] != apps["high"],
            "hires": hires_is_range,
            "cpa": cpa["low"] != cpa["high"],
            "cph": cph["low"] != cph["high"],
        },
        "assumptions": assumptions,
        "notes": notes,
        "text": describe(
            {
                "applications": apps,
                "hires": hires,
                "cpa": cpa,
                "cph": cph,
                "assumptions": assumptions,
                "notes": notes,
            },
            symbol=symbol,
        ),
    }


def projection_for_alloc(
    alloc: Dict[str, Any],
    *,
    budget: float,
    industry_key: str,
    role_titles: Optional[List[str]] = None,
    symbol: Optional[str] = None,
    display_currency: Optional[str] = None,
    non_us_market: bool = False,
    mixed_market: bool = False,
) -> Dict[str, Any]:
    """The projection a deliverable (workbook, deck) shows for an engine result.

    ``alloc["headline_projection"]`` -- stamped at /api/generate time by
    ``project_plan`` with the wizard preview's inputs (no live enrichment) -- is
    the headline: live market data may refine channel detail but never moves
    these ranges. Used when it is present and in the deliverable's currency;
    otherwise the ranges are built from the engine result itself."""
    hp = alloc.get("headline_projection") if isinstance(alloc, dict) else None
    if isinstance(hp, dict) and hp.get("ok") and hp.get("applications"):
        code = str((hp.get("currency") or {}).get("code") or "USD").upper()
        if not display_currency or str(display_currency).upper() == code:
            return hp
    return ranges_from_engine_result(
        alloc,
        budget=budget,
        industry_key=industry_key,
        role_titles=role_titles,
        symbol=symbol,
        display_currency=display_currency,
        non_us_market=non_us_market,
        mixed_market=mixed_market,
    )


# --------------------------------------------------------------------------
# hires claims -- removed from prose where hires are not projected
# --------------------------------------------------------------------------

# A sentence that STATES a projected hire count / cost per hire ...
_HIRE_CLAIM = re.compile(
    r"\bprojected\s+(?:to\s+(?:deliver|drive)|[\d,]+\s+hires?)"
    r"|\bprojected hires?\s+(?:drop|fall|decline|rise|increase|of|at|is|are|will)\b"
    r"|\b[\d,]+\s+projected\s+hires?"
    r"|\bprojects?\s+[\d,]+"
    r"|\bdeliver\s+[\d,]+"
    r"|\bbuys\s+[\d,]+"
    r"|\bshows?\s+[\d,]+\s+hires?"
    r"|\bimpact\s+[\d,]+\s+(?:projected\s+)?hires?"
    r"|\bexpect\s+[\d,]+\s+additional hire"
    r"|hires?\s+(?:drop|short)"
    r"|fewer hires?"
    r"|hire gap"
    r"|[\u00a3$\u20ac\u20b9\u00a5]\s?[\d,.]+[KkMm]?\s*(?:/|per\s+)hire\b"
    r"|cost[- ]per[- ]hire\s+(?:is|of)\s+[\u00a3$\u20ac\u20b9\u00a5]",
    re.I,
)
# ... unless it is the sentence that DECLARES the suppression.
_HIRE_DECLARE = re.compile(r"not projected|no local|no verified|not assessed|indicative", re.I)
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9\u25b8\u2713\u2022(\u2014-])")
# inline tails that can be cut without losing the rest of the sentence
_HIRE_TAILS = (
    re.compile(r"\s*[\u2014-]\s*this plan projects [\d,]+ \(\d+%\)", re.I),
    re.compile(r"\s+to project [\d,]+ hires?\b", re.I),
    re.compile(r"\s*\(\s*[\d,]+\s+hires?\s*/\s*[\d,]+\s+applications?\s*\)", re.I),
)


def strip_hire_claims(text: Any) -> str:
    """``text`` without any sentence that states a projected hire count or a
    per-hire cost (a market with no local cost-per-hire benchmark projects
    none). Sentences that declare the suppression are kept; a client's own
    stated goal ("100 hires target") is not a projection and is kept."""
    if not isinstance(text, str) or not text:
        return text if isinstance(text, str) else ""
    out = text
    for tail in _HIRE_TAILS:
        out = tail.sub("", out)
    kept = []
    for sent in _SENTENCE_SPLIT.split(out):
        if _HIRE_CLAIM.search(sent) and not _HIRE_DECLARE.search(sent):
            continue
        kept.append(sent)
    return " ".join(kept).strip()


# --------------------------------------------------------------------------
# display strings -- ONE wording for every surface
# --------------------------------------------------------------------------


def range_text(rng: Dict[str, Any], kind: str = "count", symbol: str = "$") -> str:
    """"9-19" / "$3,158-$6,667" / "$6.47-$35"; a single value when low == high;
    "n/a" when the range is empty. En dash, as the deck and workbook print."""
    lo, hi = rng.get("low"), rng.get("high")
    if lo is None or hi is None:
        return "n/a"

    def one(v: float) -> str:
        if kind == "count":
            return f"{int(round(v)):,}"
        if kind == "cpa":
            return _money(v, symbol)
        return f"{symbol}{v:,.0f}"

    if one(lo) == one(hi):
        return one(lo)
    return f"{one(lo)}–{one(hi)}"


def describe(proj: Dict[str, Any], symbol: str = "$") -> Dict[str, str]:
    """Ready-to-print strings for a projection (all surfaces use these)."""
    a = proj.get("assumptions") or {}
    hires_txt = range_text(proj["hires"], "count", symbol)
    cph_txt = range_text(proj["cph"], "cph", symbol)
    apps_txt = range_text(proj["applications"], "count", symbol)
    cpa_txt = range_text(proj["cpa"], "cpa", symbol)
    if a.get("hires_suppressed"):
        hires_label = cph_label = NO_LOCAL_HIRES_NOTE
    elif a.get("below_one_hire"):
        hires_label = (
            f"Estimated hires {hires_txt} (budget is below one hire at industry "
            "cost per hire; no client hire history provided)"
        )
        cph_label = "Estimated cost per hire n/a (budget is below one hire at industry cost per hire)"
    else:
        hires_label = f"Estimated hires {hires_txt} (assumes industry cost per hire; no client hire history provided)"
        cph_label = f"Estimated cost per hire {cph_txt} (total cost-per-hire basis, not media-only)"
    kb = a.get("kb_cpa") or {}
    cpa_short = ""
    if kb:
        for n in proj.get("notes") or []:
            if "per application" in n:
                cpa_short = n.split(" (", 1)[0].rstrip(".") + "."
                break
    return {
        "applications": apps_txt,
        "hires": hires_txt,
        "cpa": cpa_txt,
        "cph": cph_txt,
        "hires_label": hires_label,
        "cph_label": cph_label,
        "hires_note": NO_LOCAL_HIRES_NOTE if a.get("hires_suppressed") else "",
        "hires_short": (
            NO_LOCAL_HIRES_NOTE
            if a.get("hires_suppressed")
            else "Budget is below one hire at industry cost per hire; no client hire "
            "history provided."
            if a.get("below_one_hire")
            else (
                "Hires are an estimate (budget \u00f7 industry cost per hire, "
                "US benchmark \u2014 no local data); no client hire "
                "history provided."
                if a.get("us_benchmark_no_local")
                else (
                    "Hires are an estimate (budget \u00f7 industry cost per hire; "
                    + MIXED_MARKETS_LABEL
                    + "); no client hire history provided."
                    if a.get("mixed_markets")
                    else "Hires are an estimate (budget \u00f7 industry cost per hire); "
                    "no client hire history provided."
                )
            )
        ),
        "cph_short": (
            NO_LOCAL_HIRES_NOTE
            if a.get("hires_suppressed")
            else "US benchmark \u2014 no local data; total cost-per-hire basis"
            if a.get("us_benchmark_no_local")
            else (
                "total cost-per-hire basis, not media-only; " + MIXED_MARKETS_LABEL
                if a.get("mixed_markets")
                else "total cost-per-hire basis, not media-only"
            )
        ),
        "hires_basis": str(a.get("hires_basis") or ""),
        "cph_basis": str(a.get("cph_basis") or ""),
        "cpa_note": cpa_short,
    }


def public_view(proj: Dict[str, Any]) -> Dict[str, Any]:
    """The JSON-safe slice of a projection that API responses and tool results
    carry (everything except the raw engine result)."""
    keep = (
        "budget", "currency", "industry", "industry_source", "applications", "hires", "cpa",
        "cph", "engine", "is_range", "assumptions", "notes", "text",
    )
    return {k: proj[k] for k in keep if k in proj}


# --------------------------------------------------------------------------
# the front door
# --------------------------------------------------------------------------


def classify_brief_industry(
    industry: Any, client_name: Any = "", role_titles: Optional[List[str]] = None
) -> Tuple[str, str]:
    """(engine industry key, source) for a brief.

    Uses the app's one classifier (name + roles + explicit industry). Never
    returns "general" when a role signal exists -- the classifier reads roles
    -- and reports ``source`` so a caller can say the industry was inferred.
    """
    raw = str(industry or "").strip()
    # "general" / "other" / "n/a" mean "not stated" (it is what an LLM tool
    # caller fills in when it has no industry): let the roles speak instead of
    # taking it as an explicit General pick, which the classifier would honour.
    generic = raw.lower() in ("", "general", "other", "n/a", "na", "none")
    if generic:
        raw = ""
    try:
        app_mod = sys.modules.get("app")
        if app_mod is None:
            import app as app_mod  # type: ignore[no-redef]
        profile = app_mod.classify_industry(raw, str(client_name or ""), list(role_titles or []))
        key = str((profile or {}).get("legacy_key") or "").strip()
        if key:
            return key, ("classified_from_roles" if generic else "brief")
    except Exception as exc:  # noqa: BLE001 - classifier import is best-effort
        logger.warning("plan_projection: industry classifier unavailable: %s", exc)
    return _FALLBACK_INDUSTRY, "default"


def default_channel_percentages(industry_key: str, region: str = "us_only") -> Dict[str, float]:
    """The wizard's default channel mix for ``industry_key`` (APAC/EMEA folded
    into the largest channel for a US-only plan, exactly as the wizard does)."""
    pcts: Dict[str, float] = dict(_DEFAULT_ALLOC)
    try:
        from ppt_generator import INDUSTRY_ALLOC_PROFILES

        pcts = dict(INDUSTRY_ALLOC_PROFILES.get(industry_key, _DEFAULT_ALLOC))
    except Exception as exc:  # noqa: BLE001
        logger.debug("plan_projection: allocation profiles unavailable: %s", exc)
    if region == "us_only":
        intl = pcts.pop("apac_regional", 0) + pcts.pop("emea_regional", 0)
        if intl > 0 and pcts:
            top = max(pcts, key=lambda k: pcts[k])
            pcts[top] = pcts[top] + intl
    return pcts


def _role_dicts(roles: Any) -> Tuple[List[Dict[str, Any]], List[str]]:
    out: List[Dict[str, Any]] = []
    titles: List[str] = []
    for r in list(roles or [])[:50]:
        if isinstance(r, str):
            title = r.strip()
            if title:
                titles.append(title)
                out.append({"title": title, "count": 1, "tier": "Professional"})
        elif isinstance(r, dict):
            title = str(r.get("title") or r.get("role") or "").strip()
            if title:
                titles.append(title)
                try:
                    count = int(r.get("count", 1) or 1)
                except (TypeError, ValueError):
                    count = 1
                out.append(
                    {
                        "title": title,
                        "count": count,
                        "tier": str(r.get("tier") or r.get("_tier") or "Professional"),
                    }
                )
    return out, titles


def project_plan(
    brief: Dict[str, Any],
    *,
    channel_percentages: Optional[Dict[str, float]] = None,
    vendor_availability: Optional[Dict[str, bool]] = None,
    knowledge_base: Optional[Dict[str, Any]] = None,
    plan_currency: Optional[str] = None,
    industry_key: Optional[str] = None,
) -> Dict[str, Any]:
    """THE projection: low / mid / high for applications, hires, cost per
    application and cost per hire, plus the assumptions behind them.

    ``brief`` keys: ``budget`` (campaign total, number), ``roles`` (titles or
    dicts), ``locations`` (strings or dicts), ``industry`` (optional),
    ``client_name`` (optional), ``campaign_start_month``, ``target_hires``,
    ``budget_text``, ``locations_raw``. The keyword arguments let the wizard
    pass what it already resolved (region fold, channel toggles, vendor gate);
    left out, they take the wizard's defaults, so a bare brief and an
    untouched wizard give the SAME numbers.
    """
    try:
        import budget_engine

        budget = _num(brief.get("budget"))
        if not budget or budget <= 0:
            return {"ok": False, "error": "Budget must be greater than zero"}
        roles, titles = _role_dicts(brief.get("roles") or brief.get("target_roles"))
        if not roles:
            return {"ok": False, "error": "At least one role is required"}
        locs_in = brief.get("locations") or []
        locations = [split_location(loc) for loc in list(locs_in)[:50]]

        source = "brief"
        key = industry_key
        if not key:
            key, source = classify_brief_industry(
                brief.get("industry"), brief.get("client_name"), titles
            )
        kb = _load_kb(knowledge_base)
        region = str(brief.get("target_region") or "us_only")
        pcts = channel_percentages or default_channel_percentages(key, region)

        if vendor_availability is None:
            try:
                import excel_v2
                import plan_geo

                # the SAME region-aware check the wizard makes: its brief
                # carries target_region (default "us_only"), which is_us_plan
                # honours first -- without it Quick Plan / Nova gated niche
                # boards off for a London plan the wizard funded.
                us_plan = plan_geo.is_us_plan(
                    {
                        "locations": brief.get("locations_raw") or locs_in,
                        "target_region": region,
                    }
                )
                vendor_availability = excel_v2.get_niche_vendor_availability(
                    industry=key, us_plan=us_plan
                )
            except Exception as exc:  # noqa: BLE001 - gate is optional
                logger.debug("plan_projection: vendor gate unavailable: %s", exc)
                vendor_availability = None

        if plan_currency is None:
            try:
                import plan_currency as _pc

                plan_currency, _basis = _pc.currency_for_plan_with_basis(
                    {
                        "budget": brief.get("budget_text") or "",
                        "locations": brief.get("locations_raw") or locs_in,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug("plan_projection: currency resolver unavailable: %s", exc)
                plan_currency = None

        result = budget_engine.calculate_budget_allocation(
            total_budget=budget,
            roles=roles,
            locations=locations,
            industry=key,
            channel_percentages=pcts,
            synthesized_data=None,
            knowledge_base=kb or None,
            collar_type="",
            campaign_start_month=int(brief.get("campaign_start_month") or 0),
            vendor_availability=vendor_availability,
            plan_currency=plan_currency or None,
            locations_raw=brief.get("locations_raw") or list(locs_in),
            budget_text=str(brief.get("budget_text") or "") or None,
            target_hires=int(brief.get("target_hires") or 0) or None,
        )
        market_locs = brief.get("locations_raw") or locs_in
        non_us_market = is_non_us_market(market_locs)
        proj = ranges_from_engine_result(
            result,
            budget=budget,
            industry_key=key,
            role_titles=titles,
            knowledge_base=kb,
            non_us_market=non_us_market,
            mixed_market=is_mixed_market(brief.get("locations_raw") or locs_in),
        )
        proj["industry_source"] = source
        proj["budget_result"] = result
        return proj
    except Exception as exc:  # noqa: BLE001 - a projection must never crash a caller
        logger.error("project_plan failed: %s", exc, exc_info=True)
        return {"ok": False, "error": "Projection failed"}
