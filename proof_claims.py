"""proof_claims.py -- the single source of truth for every public proof number.

Every number a visitor reads on the Nova hub (templates/hub.html) or in
Nova's chat greeting (nova.py) -- "7,053 supply partners", "26 AI models" --
is computed HERE, once per process, from a real artifact in this repository,
and each value carries the file or function it was read from.  Nothing on
those surfaces hardcodes a count any more: the templates carry
``{{claim_<key>}}`` tokens that app.py substitutes when it serves them, and
nova.py builds its greeting from the same dict.

Fail-safe: when a source cannot be read the claim is OMITTED -- the template
block that wraps it is dropped and the greeting leaves that stat out --
rather than falling back to a number that may have gone stale.  A wrong
number on a public surface is worse than a missing one.

Template syntax (handled by :func:`render`):

    {{claim_supply_partners}}            -> "7,053"
    {{claim_supply_partners:raw}}        -> "7053"   (for data-* attributes)
    {{#claim_x}}...{{/claim_x}}          -> kept only when claim x exists
    {{#claim_x+y}}...{{/claim_x+y}}      -> kept only when ALL of x, y exist
    {{^claim_x}}...{{/claim_x}}          -> kept only when claim x is MISSING

Audit the live values and their sources at GET /api/proof-claims.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.path.join(_BASE_DIR, "data")

# Measured, not derived from code: the plan-generation time comes from the
# committed measurement in data/plan_generation_timing.json (notifier timings
# of real plans) and renders as prose ("about a minute"), never as a hard
# number the page could silently outgrow.  Re-measure to update the file.
_PLAN_TIME_FILE = "plan_generation_timing.json"


def _plan_time_measurement() -> Dict[str, Any]:
    with open(os.path.join(_DATA_DIR, _PLAN_TIME_FILE), "r", encoding="utf-8") as f:
        m = json.load(f)
    p50 = m.get("p50_seconds")
    n = m.get("n")
    if not isinstance(p50, (int, float)) or not isinstance(n, int) or n < 20:
        raise ValueError(f"{_PLAN_TIME_FILE}: need p50_seconds and n>=20, got {m!r}")
    return m


def _plan_time_text(p50: float) -> tuple:
    nbsp = "\u00a0"  # the short form sits in a stat tile; never let it wrap
    if p50 < 40:
        return "under a minute", f"<1{nbsp}min"
    if p50 <= 90:
        return "about a minute", f"~1{nbsp}min"
    minutes = max(2, round(p50 / 60))
    return f"about {minutes} minutes", f"~{minutes}{nbsp}min"


def _plan_time_source(m: Dict[str, Any]) -> str:
    return (
        f"data/{_PLAN_TIME_FILE}: {m.get('source')}, {m.get('window')}, "
        f"n={m.get('n')}, p50 {m.get('p50_seconds')} s, p90 {m.get('p90_seconds')} s "
        f"(measured {m.get('measured_on')})"
    )


@dataclass(frozen=True)
class ProofClaim:
    """One public number and where it came from."""

    key: str
    value: Optional[int]  # None for a documented (measured) claim such as plan time
    text: str  # exactly how it renders on the page
    source: str  # file / function the value was read from
    floor: bool = False  # True when the true figure is at least ``value``

    @property
    def raw(self) -> str:
        return str(self.value) if self.value is not None else self.text

    def as_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "text": self.text,
            "source": self.source,
            "floor": self.floor,
        }


def _fmt(value: int, floor: bool = False) -> str:
    return f"{value:,}" + ("+" if floor else "")


def _counted(key: str, value: int, source: str, floor: bool = False) -> ProofClaim:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{key}: expected a positive int, got {value!r}")
    return ProofClaim(
        key=key, value=value, text=_fmt(value, floor), source=source, floor=floor
    )


# ── Individual sources ───────────────────────────────────────────────────────
# Each returns a ProofClaim or raises; claims() turns a raise into an omission.


def _supply_partners() -> ProofClaim:
    path = os.path.join(_DATA_DIR, "joveo_global_supply_repository.json")
    with open(path, "r", encoding="utf-8") as f:
        repo = json.load(f)
    active = repo.get("active_publishers")
    total = repo.get("total_publishers")
    if not isinstance(active, int):
        raise ValueError(f"active_publishers is {active!r}")
    return _counted(
        "supply_partners",
        active,
        "data/joveo_global_supply_repository.json: active_publishers "
        f"(of {total:,} listed; deactivated, churned and paused partners excluded)",
    )


_MODEL_NOISE = ("-instruct", "-turbo", "-fp8-fast", ":free")


def _canonical_model(model_id: str) -> str:
    """``@cf/meta/llama-3.3-70b-instruct-fp8-fast`` and ``Meta-Llama-3.3-70B-Instruct``
    are one model on two hosts; fold host prefixes and serving suffixes away."""
    name = model_id.strip().lower().rsplit("/", 1)[-1]
    for noise in _MODEL_NOISE:
        name = name.replace(noise, "")
    if name.startswith("meta-"):
        name = name[len("meta-") :]
    return name


def _ai_models() -> ProofClaim:
    import llm_router  # local import: heavy module, and avoids import cycles

    models = {
        _canonical_model(str(cfg.get("model") or ""))
        for cfg in llm_router.PROVIDER_CONFIG.values()
        if isinstance(cfg, dict)
    }
    models.discard("")
    return _counted(
        "ai_models",
        len(models),
        "llm_router.PROVIDER_CONFIG: distinct models across registered providers "
        "(the same model served by several hosts counts once)",
    )


def _live_apis() -> ProofClaim:
    import api_integrations
    import recruitment_apis

    registry = api_integrations.get_api_status()
    configured = [k for k, v in registry.items() if v.get("configured")]
    keyless = recruitment_apis.LIVE_SOURCES
    return _counted(
        "live_apis",
        len(configured) + len(keyless),
        "api_integrations.get_api_status(): integrations enabled in this environment "
        f"(keyless, or with credentials configured: {len(configured)} of {len(registry)} "
        f"registered) + recruitment_apis.LIVE_SOURCES keyless live endpoints ({len(keyless)})",
    )


def _knowledge_bases() -> ProofClaim:
    import kb_loader

    present = [
        fn
        for fn in dict.fromkeys(kb_loader.KB_FILES.values())
        if os.path.isfile(os.path.join(_DATA_DIR, fn))
    ]
    return _counted(
        "knowledge_bases",
        len(present),
        "kb_loader.KB_FILES: data/*.json files present on disk and loaded at startup",
    )


_COUNTRY_ALIASES = {
    "uk": "united kingdom",
    "uae": "united arab emirates",
    "us": "united states",
    "usa": "united states",
}


def _norm_country(name: str) -> str:
    key = name.strip().lower().replace("_", " ")
    return _COUNTRY_ALIASES.get(key, key)


def _countries() -> ProofClaim:
    """Distinct countries keyed in the country-level benchmark and job-board KBs."""
    sources = (
        ("international_benchmarks_2026.json", ("countries",)),
        ("global_supply.json", ("country_job_boards",)),
        ("joveo_publishers.json", ("by_country",)),
    )
    seen: set = set()
    for filename, keys in sources:
        with open(os.path.join(_DATA_DIR, filename), "r", encoding="utf-8") as f:
            data = json.load(f)
        node: Any = data
        for k in keys:
            node = node.get(k) if isinstance(node, dict) else None
        if not isinstance(node, dict) or not node:
            raise ValueError(f"{filename}: {'.'.join(keys)} is empty or not a dict")
        seen.update(_norm_country(k) for k in node if isinstance(k, str) and k.strip())
    return _counted(
        "countries",
        len(seen),
        "distinct country keys across data/international_benchmarks_2026.json "
        "(countries), data/global_supply.json (country_job_boards) and "
        "data/joveo_publishers.json (by_country)",
    )


# Tools whose backing source is a URL stub or a paid key-gated stub (see
# recruitment_apis.LIVE_SOURCES); registered for the LLM, but not live.
_STUB_TOOL_MARKERS = ("warntracker", "levels", "crunchbase", "pdl")


def _ai_tools() -> ProofClaim:
    import nova  # local import: nova imports this module

    tools = nova.Nova.get_tool_definitions(None)  # the method does not touch self
    names = {t.get("name") for t in tools if isinstance(t, dict) and t.get("name")}
    live = {n for n in names if not any(marker in n for marker in _STUB_TOOL_MARKERS)}
    return _counted(
        "ai_tools",
        len(live),
        "nova.Nova.get_tool_definitions(): distinct registered tool names, minus the "
        f"{len(names) - len(live)} URL-stub / key-gated placeholder tools",
    )


def _llm_providers() -> ProofClaim:
    import llm_router

    from urllib.parse import urlparse

    # A provider is an API host (one per company), not a router entry: OpenRouter
    # alone has 8 entries and Anthropic 3.
    hosts = set()
    for key, cfg in llm_router.PROVIDER_CONFIG.items():
        if not isinstance(cfg, dict):
            continue
        host = urlparse(str(cfg.get("endpoint") or "")).netloc.lower()
        hosts.add(host or key)
    return _counted(
        "llm_providers",
        len(hosts),
        "llm_router.PROVIDER_CONFIG: distinct API hosts across the registered "
        "endpoints (several router entries share one provider)",
    )


def _jobs_analyzed() -> ProofClaim:
    path = os.path.join(_DATA_DIR, "slotops_benchmarks_summary.json")
    with open(path, "r", encoding="utf-8") as f:
        meta = json.load(f).get("metadata") or {}
    total = meta.get("total_jobs")
    if not isinstance(total, int):
        raise ValueError(f"metadata.total_jobs is {total!r}")
    return _counted(
        "jobs_analyzed",
        total,
        "data/slotops_benchmarks_summary.json: metadata.total_jobs (LinkedIn job "
        "postings in the SlotOps benchmark dataset)",
    )


def _plan_time() -> ProofClaim:
    m = _plan_time_measurement()
    text, _short = _plan_time_text(float(m["p50_seconds"]))
    return ProofClaim(
        key="plan_time", value=None, text=text, source=_plan_time_source(m)
    )


def _plan_time_short() -> ProofClaim:
    m = _plan_time_measurement()
    _text, short = _plan_time_text(float(m["p50_seconds"]))
    return ProofClaim(
        key="plan_time_short", value=None, text=short, source=_plan_time_source(m)
    )


# Company-level certification, attributed to the company: Joveo (the company)
# is SOC 2 Type II certified; Nova itself has not been separately audited, so
# the chip must not read as a claim about Nova.
_SOC2_SOURCE = (
    "https://www.joveo.com/security-and-compliance/ -- Joveo the company; "
    "Nova itself has not been separately audited"
)


def _soc2() -> ProofClaim:
    return ProofClaim(
        key="soc2",
        value=None,
        text="Built at Joveo (SOC 2 Type II)",
        source=_SOC2_SOURCE,
    )


_SOURCES: Dict[str, Callable[[], ProofClaim]] = {
    "supply_partners": _supply_partners,
    "ai_models": _ai_models,
    "live_apis": _live_apis,
    "knowledge_bases": _knowledge_bases,
    "countries": _countries,
    "ai_tools": _ai_tools,
    "llm_providers": _llm_providers,
    "jobs_analyzed": _jobs_analyzed,
    "plan_time": _plan_time,
    "plan_time_short": _plan_time_short,
    "soc2": _soc2,
}

# Order of the stat line in Nova's greeting; keys missing at runtime are skipped.
_GREETING_ORDER = (
    ("supply_partners", "supply partners"),
    ("live_apis", "live APIs"),
    ("ai_models", "AI models"),
    ("knowledge_bases", "knowledge bases"),
    ("countries", "countries"),
)

# ── Cache ────────────────────────────────────────────────────────────────────

_cache: Optional[Dict[str, ProofClaim]] = None
_cache_lock = threading.RLock()
_computed_at: Optional[float] = None
_computing = threading.local()  # re-entrancy guard (a source must not read the claims)


def compute() -> Dict[str, ProofClaim]:
    """Compute every claim from its source.  Unreadable sources are omitted."""
    out: Dict[str, ProofClaim] = {}
    _computing.active = True
    try:
        for key, fn in _SOURCES.items():
            try:
                out[key] = fn()
            except (
                Exception
            ) as exc:  # noqa: BLE001 -- any failure means "omit", never "guess"
                logger.error(
                    "proof_claims: %s omitted -- source unreadable: %s",
                    key,
                    exc,
                    exc_info=True,
                )
    finally:
        _computing.active = False
    logger.info(
        "proof_claims: %s",
        "; ".join(f"{k}={c.text} <- {c.source}" for k, c in out.items())
        or "none computed",
    )
    return out


def claims() -> Dict[str, ProofClaim]:
    """Cached per process.  Call :func:`refresh` to recompute."""
    global _cache, _computed_at
    if _cache is None:
        if getattr(_computing, "active", False):
            # A source function asked for the claims while they are being
            # computed (e.g. nova's tool list rendering a greeting). Answer
            # "nothing yet" instead of recursing or deadlocking.
            logger.warning(
                "proof_claims: re-entrant claims() during compute; returning empty"
            )
            return {}
        with _cache_lock:
            if _cache is None:
                _cache = compute()
                _computed_at = time.time()
    return _cache


def refresh() -> Dict[str, ProofClaim]:
    global _cache, _computed_at
    with _cache_lock:
        _cache = compute()
        _computed_at = time.time()
    return _cache


def claim_text(key: str, default: str = "") -> str:
    c = claims().get(key)
    return c.text if c else default


def as_dict() -> Dict[str, Any]:
    """JSON-ready view for /api/proof-claims."""
    c = claims()
    return {
        "claims": {k: v.as_dict() for k, v in c.items()},
        "omitted": sorted(set(_SOURCES) - set(c)),
        "computed_at": _computed_at,
        "note": (
            "Every number is computed from the named source at startup; "
            "a source that cannot be read is omitted, never replaced by a stale value."
        ),
    }


# ── Rendering ────────────────────────────────────────────────────────────────

# What an UNGUARDED token renders as when its claim is missing, so prose stays
# grammatical ("across thousands of supply partners") without inventing a
# number. Guarded blocks ({{#claim_x}}...{{/claim_x}}) are dropped instead.
_FALLBACK_TEXT: Dict[str, str] = {
    "supply_partners": "thousands of",
    "countries": "dozens of",
    "knowledge_bases": "dozens of",
    "ai_models": "multiple",
    "llm_providers": "multiple",
    "live_apis": "multiple",
    "ai_tools": "dozens of",
    "jobs_analyzed": "over a hundred thousand",
    "plan_time": "about a minute",
    "plan_time_short": "~1\u00a0min",
    "soc2": "Built at Joveo",
}

_BLOCK_RE = re.compile(
    r"\{\{([#^])claim_([a-z0-9_]+(?:\+[a-z0-9_]+)*)\}\}(.*?)\{\{/claim_\2\}\}", re.S
)
_TOKEN_RE = re.compile(r"\{\{claim_([a-z0-9_]+)(?::(raw))?\}\}")


def render(text: str, values: Optional[Dict[str, ProofClaim]] = None) -> str:
    """Substitute claim blocks and tokens in ``text`` (see module docstring)."""
    c = claims() if values is None else values

    def _block(m: "re.Match[str]") -> str:
        kind, keys, inner = m.group(1), m.group(2).split("+"), m.group(3)
        present = all(k in c for k in keys)
        return inner if present == (kind == "#") else ""

    # Blocks may nest (different keys); re-run until the text settles.
    for _ in range(6):
        new = _BLOCK_RE.sub(_block, text)
        if new == text:
            break
        text = new

    def _token(m: "re.Match[str]") -> str:
        key = m.group(1)
        claim = c.get(key)
        if claim is None:
            fallback = _FALLBACK_TEXT.get(key, "")
            logger.warning(
                "proof_claims: {{claim_%s}} has no value; rendered as %r", key, fallback
            )
            return fallback
        return claim.raw if m.group(2) == "raw" else claim.text

    return _TOKEN_RE.sub(_token, text)


def render_bytes(content: bytes) -> bytes:
    """``render`` for a served template body; untouched when it carries no tokens."""
    if b"{{claim_" not in content:
        return content
    try:
        return render(content.decode("utf-8")).encode("utf-8")
    except UnicodeDecodeError:
        return content


# ── Prose helpers for nova.py ────────────────────────────────────────────────


def greeting_stats_line(bold: str = "**") -> str:
    """``**7,053 supply partners** | **15 live APIs** | ...`` from available claims."""
    c = claims()
    parts = [
        f"{bold}{c[k].text} {label}{bold}" for k, label in _GREETING_ORDER if k in c
    ]
    return " | ".join(parts)


def reach_phrase() -> str:
    """``7,053 supply partners and country-level benchmarks for 46 countries``.

    Degrades clause by clause as claims go missing; never invents a number.
    """
    c = claims()
    supply = c.get("supply_partners")
    countries = c.get("countries")
    if supply and countries:
        return (
            f"{supply.text} supply partners and job-board and benchmark coverage "
            f"for {countries.text} countries"
        )
    if supply:
        return f"{supply.text} supply partners"
    if countries:
        return f"job-board and benchmark coverage for {countries.text} countries"
    return "Joveo's global supply partner network"


def countries_phrase(prefix: str = "for") -> str:
    """``for 46 countries`` or ``worldwide`` when the count is unavailable."""
    countries = claims().get("countries")
    return f"{prefix} {countries.text} countries" if countries else "worldwide"
