"""Layer-1 typed pipeline contract for the media-plan enrichment payload.

This module is the foundation typed contract for the dict that flows through the
generation pipeline::

    enrich  ->  synthesize  ->  budget  ->  generators (excel/pptx/pdf/deck)

It is *intentionally* built on the standard-library ``dataclasses`` module (not
pydantic) to keep the dependency surface minimal -- the server runs on the
Python stdlib by design (see CLAUDE.md).

Design goals
------------
1. **Describe, don't enforce.** The pipeline already passes plain ``dict`` s
   around and many modules read them with ``.get(...)``. These dataclasses model
   that shape so new code can adopt typing incrementally, while
   ``to_dict()`` round-trips back to the exact dict shape downstream readers
   (excel_v2, budget_engine, scorecard_generator, ...) already expect.

2. **Never crash on schema drift.** Generators run at the end of an expensive
   pipeline; a missing or mistyped field must never raise. ``from_dict()`` is a
   tolerant constructor and :func:`validate_and_normalize` coerces/defaults and
   *returns* warnings rather than raising.

3. **Provenance-ready.** Every :class:`ChannelAllocation` carries
   ``source`` / ``vintage`` / ``confidence`` provenance fields so downstream
   "where did this number come from" features (L1 provenance work) have a place
   to read from. These mirror the keys returned by
   ``supabase_data.get_real_outcomes`` (``source``, ``last_updated``,
   ``confidence``).

Real-world field names are mirrored from the live payload:
- per-channel keys from ``budget_engine.compute_channel_dollar_amounts``
- ``_budget_allocation`` / ``_enriched`` / ``_validation`` from
  ``excel_v2`` normalization.

Python 3.9 compatible (uses ``from __future__ import annotations``).
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Any, Dict, List, Optional, Tuple

__all__ = [
    "ChannelAllocation",
    "BudgetAllocation",
    "PlanData",
    "validate_and_normalize",
]

# Confidence labels seen across the codebase (budget_engine emits low/medium/high;
# supabase_data emits "measured"). We normalize-to-lower but never reject unknowns.
_KNOWN_CONFIDENCE = {"low", "medium", "high", "measured", "estimated", "modeled"}


# --------------------------------------------------------------------------- #
# Coercion helpers (tolerant -- never raise)
# --------------------------------------------------------------------------- #
def _to_float(value: Any, default: float = 0.0) -> float:
    """Best-effort float coercion. Strips ``$``/``,``/``%`` from strings."""
    if value is None:
        return default
    if isinstance(value, bool):
        # Guard: bool is an int subclass; don't silently turn True into 1.0.
        return default
    if isinstance(value, (int, float)):
        try:
            f = float(value)
        except (ValueError, OverflowError):
            return default
        # NaN/inf are not useful downstream; fall back to default.
        if f != f or f in (float("inf"), float("-inf")):
            return default
        return f
    if isinstance(value, str):
        cleaned = value.strip().replace("$", "").replace(",", "").replace("%", "")
        cleaned = cleaned.strip()
        if not cleaned or cleaned in ("-", "—", "N/A", "TBD"):
            return default
        try:
            return float(cleaned)
        except ValueError:
            return default
    return default


def _to_int(value: Any, default: int = 0) -> int:
    """Best-effort int coercion via :func:`_to_float`."""
    f = _to_float(value, float(default))
    try:
        return int(round(f))
    except (ValueError, OverflowError):
        return default


def _to_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, str):
        return value
    return str(value)


def _to_str_list(value: Any) -> List[str]:
    """Normalize the polymorphic ``roles``/``locations`` field to ``list[str]``.

    Accepts ``None`` -> ``[]``, a bare string -> ``[s]``, a list of strings, or a
    list of dicts (extracts a human-readable label). Mirrors the leniency in
    ``excel_v2`` which accepts both ``"Dallas"`` and ``{"city": "Dallas"}``.
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, dict):
        value = [value]
    if not isinstance(value, (list, tuple)):
        return [str(value)]
    out: List[str] = []
    for item in value:
        if item is None:
            continue
        if isinstance(item, str):
            if item.strip():
                out.append(item)
        elif isinstance(item, dict):
            label = (
                item.get("title")
                or item.get("name")
                or item.get("city")
                or item.get("location")
                or item.get("role")
            )
            if label:
                out.append(str(label))
            else:
                # Keep the structured entry's string form rather than dropping it.
                out.append(str(item))
        else:
            out.append(str(item))
    return out


def _as_dict(value: Any) -> Dict[str, Any]:
    """Return ``value`` if it is a dict, else an empty dict (never raise)."""
    return value if isinstance(value, dict) else {}


def _known_field_names(cls: Any) -> set:
    return {f.name for f in fields(cls)}


# --------------------------------------------------------------------------- #
# ChannelAllocation
# --------------------------------------------------------------------------- #
@dataclass
class ChannelAllocation:
    """A single advertising-channel line item in the media plan.

    Field names match the per-channel dict produced by
    ``budget_engine.compute_channel_dollar_amounts`` so this is a drop-in typed
    view over those entries.
    """

    name: str = ""
    dollar_amount: float = 0.0
    percentage: float = 0.0
    cpc: float = 0.0
    cpa: float = 0.0
    projected_clicks: int = 0
    projected_applications: int = 0
    projected_hires: int = 0
    cost_per_hire: float = 0.0
    roi_score: float = 0.0
    category: str = ""

    # --- provenance / confidence (L1 provenance-ready) ---
    # ``confidence`` is the qualitative label (low/medium/high/measured).
    confidence: str = "low"
    # ``source`` names where the headline numbers came from, e.g.
    # "Joveo Campaign Warehouse (cg_benchmarks)" or "benchmark_registry".
    source: str = ""
    # ``vintage`` is the as-of date of the source data (YYYY-MM-DD); mirrors
    # ``last_updated`` from supabase_data.get_real_outcomes.
    vintage: str = ""

    # Preserve any extra keys the pipeline attached (cpc_source, apply_rate,
    # trend_direction, efficiency_flag, ...) so to_dict() is non-lossy.
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(
        cls, payload: Optional[Dict[str, Any]], name: str = ""
    ) -> "ChannelAllocation":
        """Tolerant constructor. Unknown keys are preserved in ``extra``."""
        data = _as_dict(payload)
        known = _known_field_names(cls) - {"extra"}
        # Accept ``last_updated`` as an alias for ``vintage`` (warehouse naming).
        vintage = data.get("vintage")
        if vintage is None:
            vintage = data.get("last_updated")

        obj = cls(
            name=_to_str(data.get("name") or name),
            # ``dollars`` is a legacy alias for ``dollar_amount`` in budget_engine.
            dollar_amount=_to_float(data.get("dollar_amount", data.get("dollars"))),
            percentage=_to_float(data.get("percentage")),
            cpc=_to_float(data.get("cpc")),
            cpa=_to_float(data.get("cpa")),
            projected_clicks=_to_int(data.get("projected_clicks")),
            projected_applications=_to_int(data.get("projected_applications")),
            projected_hires=_to_int(data.get("projected_hires")),
            cost_per_hire=_to_float(data.get("cost_per_hire")),
            roi_score=_to_float(data.get("roi_score")),
            category=_to_str(data.get("category")),
            confidence=_normalize_confidence(data.get("confidence")),
            source=_to_str(data.get("source")),
            vintage=_to_str(vintage),
        )
        consumed = known | {"dollars", "last_updated"}
        obj.extra = {k: v for k, v in data.items() if k not in consumed}
        return obj

    def to_dict(self) -> Dict[str, Any]:
        """Round-trip back to the plain per-channel dict shape.

        ``extra`` keys are merged at the top level (not nested) so downstream
        readers see the same flat structure the pipeline produced. ``name`` and
        ``extra`` are not emitted as literal keys (name is the dict's key in the
        parent map; extra is spread).
        """
        out: Dict[str, Any] = dict(self.extra)
        out.update(
            {
                "dollar_amount": self.dollar_amount,
                "percentage": self.percentage,
                "cpc": self.cpc,
                "cpa": self.cpa,
                "projected_clicks": self.projected_clicks,
                "projected_applications": self.projected_applications,
                "projected_hires": self.projected_hires,
                "cost_per_hire": self.cost_per_hire,
                "roi_score": self.roi_score,
                "category": self.category,
                "confidence": self.confidence,
                "source": self.source,
                "vintage": self.vintage,
            }
        )
        return out


def _normalize_confidence(value: Any) -> str:
    """Lower-case the confidence label; default to ``"low"`` when absent."""
    s = _to_str(value).strip().lower()
    return s or "low"


# --------------------------------------------------------------------------- #
# BudgetAllocation
# --------------------------------------------------------------------------- #
@dataclass
class BudgetAllocation:
    """The ``_budget_allocation`` block: per-channel lines + aggregate totals.

    Mirrors the result of ``budget_engine.calculate_budget_allocation`` (the
    keys ``channel_allocations`` and ``total_projected`` plus assorted metadata).
    """

    channel_allocations: Dict[str, ChannelAllocation] = field(default_factory=dict)
    total_projected: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Optional[Dict[str, Any]]) -> "BudgetAllocation":
        """Tolerant constructor over a ``_budget_allocation`` dict."""
        data = _as_dict(payload)
        raw_channels = _as_dict(data.get("channel_allocations"))
        channels: Dict[str, ChannelAllocation] = {}
        for ch_name, ch_payload in raw_channels.items():
            channels[ch_name] = ChannelAllocation.from_dict(ch_payload, name=ch_name)

        # metadata is anything that isn't one of the two structured blocks; keep
        # an explicit "metadata" sub-dict merged in too if the producer used one.
        reserved = {"channel_allocations", "total_projected", "metadata"}
        metadata = {k: v for k, v in data.items() if k not in reserved}
        metadata.update(_as_dict(data.get("metadata")))

        return cls(
            channel_allocations=channels,
            total_projected=_as_dict(data.get("total_projected")),
            metadata=metadata,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Round-trip to the plain ``_budget_allocation`` dict shape."""
        out: Dict[str, Any] = dict(self.metadata)
        out["channel_allocations"] = {
            name: ch.to_dict() for name, ch in self.channel_allocations.items()
        }
        out["total_projected"] = dict(self.total_projected)
        return out

    @property
    def total_dollars(self) -> float:
        """Convenience aggregate: sum of channel dollar amounts."""
        return round(
            sum(ch.dollar_amount for ch in self.channel_allocations.values()), 2
        )


# --------------------------------------------------------------------------- #
# PlanData
# --------------------------------------------------------------------------- #
@dataclass
class PlanData:
    """Top-level media-plan payload that flows through the whole pipeline.

    The leading-underscore fields (``_budget_allocation``, ``_enriched``,
    ``_validation``) match the conventional "computed/internal" keys the
    generators read (see excel_v2). ``extra`` preserves every other key on the
    payload (``company_name``, ``work_environment``, ``_synthesized``,
    ``_role_tiers``, ``campaign_goals``, ...) so ``to_dict()`` is non-lossy.
    """

    client_name: str = "Client"
    industry: str = "general_entry_level"
    budget: Any = "Not specified"  # may be a number, a "$50k" string, or a range
    roles: List[str] = field(default_factory=list)
    locations: List[str] = field(default_factory=list)

    budget_allocation: BudgetAllocation = field(default_factory=BudgetAllocation)
    enriched: Dict[str, Any] = field(default_factory=dict)
    validation: Dict[str, Any] = field(default_factory=dict)

    extra: Dict[str, Any] = field(default_factory=dict)

    # Map dataclass attribute -> dict key (the dict uses leading underscores).
    _DICT_KEY = {
        "budget_allocation": "_budget_allocation",
        "enriched": "_enriched",
        "validation": "_validation",
    }

    @classmethod
    def from_dict(cls, payload: Optional[Dict[str, Any]]) -> "PlanData":
        """Tolerant constructor over the full plan dict."""
        data = _as_dict(payload)

        obj = cls(
            client_name=_to_str(
                data.get("client_name") or data.get("company_name"), "Client"
            )
            or "Client",
            industry=_to_str(data.get("industry"), "general_entry_level")
            or "general_entry_level",
            budget=data.get("budget", data.get("budget_range", "Not specified")),
            roles=_to_str_list(data.get("roles") or data.get("target_roles")),
            locations=_to_str_list(data.get("locations")),
            budget_allocation=BudgetAllocation.from_dict(
                data.get("_budget_allocation")
            ),
            enriched=_as_dict(data.get("_enriched")),
            validation=_as_dict(data.get("_validation")),
        )

        consumed = {
            "client_name",
            "industry",
            "budget",
            "roles",
            "locations",
            "_budget_allocation",
            "_enriched",
            "_validation",
        }
        obj.extra = {k: v for k, v in data.items() if k not in consumed}
        return obj

    def to_dict(self) -> Dict[str, Any]:
        """Round-trip back to the plain plan dict shape."""
        out: Dict[str, Any] = dict(self.extra)
        out.update(
            {
                "client_name": self.client_name,
                "industry": self.industry,
                "budget": self.budget,
                "roles": list(self.roles),
                "locations": list(self.locations),
                "_budget_allocation": self.budget_allocation.to_dict(),
                "_enriched": dict(self.enriched),
                "_validation": dict(self.validation),
            }
        )
        return out


# --------------------------------------------------------------------------- #
# validate_and_normalize -- the public, never-raising entry point
# --------------------------------------------------------------------------- #
def validate_and_normalize(
    payload: Any,
) -> Tuple[Dict[str, Any], List[str]]:
    """Coerce/default a raw plan dict and report drift -- *without raising*.

    Parses ``payload`` into the typed contract (which defaults every missing
    field) and rounds it back to a plain dict via ``to_dict()``. Along the way it
    collects human-readable ``warnings`` describing anything that looked off:
    missing core fields, empty allocations, percentage/total drift, missing
    provenance, etc.

    The pipeline must never crash on schema drift, so any internal error is
    swallowed and surfaced as a warning -- the original payload (as a dict) is
    returned in that case.

    Args:
        payload: The raw plan dict (or anything; non-dicts are tolerated).

    Returns:
        ``(normalized_dict, warnings)`` where ``normalized_dict`` is a plain dict
        safe for all downstream generators, and ``warnings`` is a (possibly
        empty) list of strings describing detected drift.
    """
    warnings: List[str] = []

    if not isinstance(payload, dict):
        warnings.append(
            f"payload is {type(payload).__name__}, expected dict; "
            "coercing to empty plan"
        )
        payload = {}

    try:
        # --- core field presence (warn on absence, defaults already applied) ---
        if not payload.get("client_name") and not payload.get("company_name"):
            warnings.append("missing client_name; defaulted to 'Client'")
        if not payload.get("industry"):
            warnings.append("missing industry; defaulted to 'general_entry_level'")
        if payload.get("budget") in (None, "", "Not specified") and not payload.get(
            "budget_range"
        ):
            warnings.append("missing budget; left as 'Not specified'")
        if not (payload.get("roles") or payload.get("target_roles")):
            warnings.append("no roles provided")
        if not payload.get("locations"):
            warnings.append("no locations provided")

        plan = PlanData.from_dict(payload)

        # --- budget allocation drift checks ---
        ba = plan.budget_allocation
        if not ba.channel_allocations:
            warnings.append("_budget_allocation has no channel_allocations")
        else:
            pct_total = round(
                sum(ch.percentage for ch in ba.channel_allocations.values()), 1
            )
            # Channel percentages should sum to ~100. Allow a tolerance band; an
            # empty/zero total means percentages were never populated.
            if pct_total == 0:
                warnings.append("channel percentages all zero")
            elif not (95.0 <= pct_total <= 105.0):
                warnings.append(
                    f"channel percentages sum to {pct_total} (expected ~100)"
                )

            for ch_name, ch in ba.channel_allocations.items():
                if not ch.source:
                    warnings.append(f"channel '{ch_name}' missing provenance source")
                if ch.confidence not in _KNOWN_CONFIDENCE:
                    warnings.append(
                        f"channel '{ch_name}' has unrecognized confidence "
                        f"'{ch.confidence}'"
                    )

            if not ba.total_projected:
                warnings.append("_budget_allocation missing total_projected aggregate")

        return plan.to_dict(), warnings

    except Exception as exc:  # never let schema drift crash the pipeline
        warnings.append(f"normalization error ({type(exc).__name__}): {exc}")
        # Return the original payload unchanged (it's a dict here) so callers
        # still get *something* usable.
        return payload, warnings
