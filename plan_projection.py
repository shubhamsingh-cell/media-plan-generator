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
    """(low, high, note) for cost per application -- see the module docstring."""
    if not kb or engine_cpa <= 0:
        return engine_cpa, engine_cpa, None
    lo, hi = float(kb["low"]), float(kb["high"])
    where = "sub-role" if kb.get("scope") == "sub_role" else "industry"
    if engine_cpa < lo:
        high = lo
        note = (
            f"Industry benchmarks run higher: {_money(lo)}-{_money(hi)} per "
            f"application ({where}; source: {kb['source']}). The modelled "
            f"{_money(engine_cpa)} is the optimistic end."
        )
        return engine_cpa, high, note
    if engine_cpa <= hi:
        return (
            engine_cpa,
            max(hi, engine_cpa),
            f"Industry benchmark range {_money(lo)}-{_money(hi)} per application "
            f"({where}; source: {kb['source']}).",
        )
    return (
        lo,
        engine_cpa,
        f"Industry benchmarks run lower: {_money(lo)}-{_money(hi)} per "
        f"application ({where}; source: {kb['source']}).",
    )


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
    symbol: str = "$",
) -> Dict[str, Any]:
    """Build the projection dict from an existing ``calculate_budget_allocation``
    result. Pure; used by ``project_plan`` and by callers that already hold an
    engine result (the wizard estimate, the workbook, the deck)."""
    tp = budget_result.get("total_projected") if isinstance(budget_result, dict) else None
    tp = tp if isinstance(tp, dict) else {}
    meta = budget_result.get("metadata") if isinstance(budget_result, dict) else None
    meta = meta if isinstance(meta, dict) else {}

    apps_engine = int(tp.get("applications") or 0)
    hires_engine = int(tp.get("hires") or 0)
    cph_engine = _num(tp.get("cost_per_hire")) or 0.0
    cpa_engine = (budget / apps_engine) if apps_engine > 0 else 0.0

    notes: List[str] = []

    # -- cost per application / applications --------------------------------
    kb_cpa = kb_cpa_range(industry_key, role_titles, knowledge_base)
    cpa_lo, cpa_hi, cpa_note = _cpa_range(cpa_engine, kb_cpa)
    if cpa_note:
        notes.append(cpa_note)
    if cpa_engine > 0:
        # The optimistic end IS the engine's own application count (when the
        # CPA low end is the engine figure), so it is never re-rounded.
        apps_hi = apps_engine if cpa_lo == cpa_engine else budget / cpa_lo
        apps = _int_range(budget / cpa_hi, apps_hi, None)
        cpa = _float_range(cpa_lo, cpa_hi, None)
    else:
        apps = _int_range(0, 0, 0)
        cpa = _float_range(None, None, None)

    # -- hires / cost per hire ----------------------------------------------
    h_lo, h_hi = tp.get("hires_low"), tp.get("hires_high")
    if isinstance(h_lo, (int, float)) and isinstance(h_hi, (int, float)) and h_hi > 0:
        hires = _int_range(h_lo, h_hi, hires_engine)
        cph = _float_range(
            budget / max(hires["high"], 1), budget / max(hires["low"], 1), cph_engine or None
        )
        hires_is_range = hires["low"] != hires["high"]
    else:
        hires = _int_range(hires_engine, hires_engine, hires_engine)
        cph = _float_range(cph_engine, cph_engine, cph_engine) if cph_engine else _float_range(None, None, None)
        hires_is_range = False
        notes.append(
            "Hires are a single estimate: this plan has no cost-per-hire benchmark "
            "in its own currency to bound a range."
        )

    # -- assumptions (what the hires / CPH numbers rest on) ------------------
    icph = meta.get("industry_cph") if isinstance(meta.get("industry_cph"), dict) else {}
    floor = _num(meta.get("cph_benchmark_floor"))
    avg = _num(icph.get("value")) or _num(meta.get("industry_avg_cph"))
    conv = kb_hire_conversion(industry_key, knowledge_base)
    assumptions: Dict[str, Any] = {
        "client_hire_history": False,
        "hires_basis": (
            "Estimated, not forecast: budget divided by the industry cost per hire "
            "from Nova's cost-per-hire table"
            + (
                f" (low end at the industry average {_money(avg, symbol)}, high end at "
                f"the plan-efficiency floor {_money(floor, symbol)} = half that average)"
                if avg and floor
                else ""
            )
            + "; no client hire history was provided."
        ),
        "cph_concept": "total",
        "cph_basis": (
            "Cost per hire here is budget / estimated hires, compared against the "
            "industry's TOTAL cost per hire (Nova's INDUSTRY_CPH_RANGES table), "
            "not a media-only figure."
        ),
        "industry_cph_avg": avg,
        "industry_cph_floor": floor,
        "kb_cpa": kb_cpa,
        **conv,
    }
    media = conv.get("media_only_cph")
    if media:
        assumptions["cph_basis"] += (
            f" The knowledge base's media-only benchmark is {media['text']} "
            f"(source: {media['source']})."
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
    hires_label = f"Estimated hires {hires_txt} (assumes industry cost per hire; no client hire history provided)"
    cph_label = f"Estimated cost per hire {cph_txt} (total cost-per-hire basis, not media-only)"
    return {
        "applications": apps_txt,
        "hires": hires_txt,
        "cpa": cpa_txt,
        "cph": cph_txt,
        "hires_label": hires_label,
        "cph_label": cph_label,
        "hires_basis": str(a.get("hires_basis") or ""),
        "cph_basis": str(a.get("cph_basis") or ""),
        "cpa_note": next((n for n in proj.get("notes") or [] if "per application" in n), ""),
    }


def public_view(proj: Dict[str, Any]) -> Dict[str, Any]:
    """The JSON-safe slice of a projection that API responses and tool results
    carry (everything except the raw engine result)."""
    keep = (
        "budget", "industry", "industry_source", "applications", "hires", "cpa",
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
        pcts = channel_percentages or default_channel_percentages(key)

        if vendor_availability is None:
            try:
                import excel_v2
                import plan_geo

                us_plan = plan_geo.is_us_plan(
                    {"locations": brief.get("locations_raw") or locs_in}
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
        proj = ranges_from_engine_result(
            result,
            budget=budget,
            industry_key=key,
            role_titles=titles,
            knowledge_base=kb,
        )
        proj["industry_source"] = source
        proj["budget_result"] = result
        return proj
    except Exception as exc:  # noqa: BLE001 - a projection must never crash a caller
        logger.error("project_plan failed: %s", exc, exc_info=True)
        return {"ok": False, "error": "Projection failed"}
