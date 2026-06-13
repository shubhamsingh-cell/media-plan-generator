"""supabase_parity.py -- S89 #9 dual-read parity audit (READ-ONLY, additive).

`supabase_data.py` already serves every domain **Supabase-first with a JSON
fallback** (it falls back only when Supabase returns *empty*). The gap that
leaves: when Supabase returns partial/wrong/empty data, the curated JSON is
never consulted and nobody notices. This module reads BOTH sources for a domain,
key-matches their rows, and reports divergence -- WITHOUT changing what `get_*`
serves to users. It builds the evidence to declare Supabase canonical with
confidence and catches silent regressions.

Domain coverage (discovered via the #9 mapping, see
`docs/_s89_9_parity_map_raw.json`):

  - **Diffable (both sources, flat rows):** channel_benchmarks, market_trends.
  - **Supabase-ONLY (JSON fallback returns []):** salary_data, compliance_rules,
    vendor_profiles -> these are a single point of failure; the audit FLAGS them
    (`verdict="supabase_only"`) so a curated fallback can be seeded.
  - **Coverage-only (fallback shape isn't row-comparable):** knowledge_base
    (the no-key fallback returns the whole file, not (category,key) rows -- the
    big trap; we audit category coverage, not values) and supply_repository
    (fallback returns ``{source_file, data}`` wrappers -- count coverage).
  - **Not eligible:** cg_benchmarks (no JSON fallback by design -- the keystone).

The diff logic (:func:`diff_rows`) is pure and side-effect-free so it can be
unit-tested against fixtures without touching Supabase or the network. All I/O
goes through ``supabase_data`` internals (``_query_supabase`` + the
``_fallback_*`` helpers), so a test can monkeypatch those.

Python stdlib only. Import-safe (no work at import time).
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

import supabase_data as sd

# How many sample mismatches / unmatched keys to include in a report (keeps the
# payload small for the admin endpoint).
_SAMPLE = 8
# Float tolerance when comparing numeric benchmark values.
_FTOL = 1e-6


# ---------------------------------------------------------------------------
# Pure helpers (unit-testable)
# ---------------------------------------------------------------------------
def _norm(value: Any) -> Any:
    """Normalize a scalar for cross-source comparison."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        try:
            return round(float(value), 6)
        except (TypeError, ValueError, OverflowError):
            return value
    if isinstance(value, str):
        return value.strip().lower()
    return value


def _row_key(row: Dict[str, Any], keys: List[str]) -> tuple:
    return tuple(_norm(row.get(k)) for k in keys)


def _values_equal(a: Any, b: Any) -> bool:
    na, nb = _norm(a), _norm(b)
    if isinstance(na, float) and isinstance(nb, float):
        return abs(na - nb) <= _FTOL
    return na == nb


def diff_rows(
    supabase_rows: List[Dict[str, Any]],
    json_rows: List[Dict[str, Any]],
    match_keys: List[str],
    compare_fields: List[str],
) -> Dict[str, Any]:
    """Diff two row sets keyed by ``match_keys``.

    Returns counts, matched/only-* keys, and a sample of value mismatches on
    ``compare_fields``. Pure: no I/O, deterministic.
    """
    sup_by_key: Dict[tuple, Dict[str, Any]] = {}
    for r in supabase_rows or []:
        if isinstance(r, dict):
            sup_by_key.setdefault(_row_key(r, match_keys), r)
    json_by_key: Dict[tuple, Dict[str, Any]] = {}
    for r in json_rows or []:
        if isinstance(r, dict):
            json_by_key.setdefault(_row_key(r, match_keys), r)

    sup_keys = set(sup_by_key)
    json_keys = set(json_by_key)
    matched = sup_keys & json_keys
    only_supabase = sup_keys - json_keys
    only_json = json_keys - sup_keys

    mismatches: List[Dict[str, Any]] = []
    for key in sorted(matched, key=lambda t: tuple("" if x is None else str(x) for x in t)):
        sr, jr = sup_by_key[key], json_by_key[key]
        diffs = {}
        for f in compare_fields:
            if not _values_equal(sr.get(f), jr.get(f)):
                diffs[f] = {"supabase": sr.get(f), "json": jr.get(f)}
        if diffs:
            mismatches.append({"key": list(key), "fields": diffs})

    def _sample_keys(s):
        return [list(k) for k in sorted(
            s, key=lambda t: tuple("" if x is None else str(x) for x in t))[:_SAMPLE]]

    return {
        "supabase_count": len(supabase_rows or []),
        "json_count": len(json_rows or []),
        "matched": len(matched),
        "only_supabase": len(only_supabase),
        "only_json": len(only_json),
        "value_mismatches": len(mismatches),
        "value_mismatch_sample": mismatches[:_SAMPLE],
        "only_supabase_sample": _sample_keys(only_supabase),
        "only_json_sample": _sample_keys(only_json),
    }


def _verdict(report: Dict[str, Any]) -> str:
    """Classify a domain report into a one-word verdict."""
    sup = report.get("supabase_count", 0)
    jsn = report.get("json_count", 0)
    if sup == 0 and jsn == 0:
        return "no_data"
    if jsn == 0:
        return "supabase_only"  # no curated fallback -> single point of failure
    if sup == 0:
        return "json_only"  # Supabase empty -> serving falls back silently
    matched = report.get("matched", 0)
    cov = matched / jsn if jsn else 0.0
    mismatch_rate = report.get("value_mismatches", 0) / matched if matched else 0.0
    if cov >= 0.9 and mismatch_rate <= 0.05:
        return "in_parity"
    if cov >= 0.5:
        return "partial_parity"
    return "diverged"


# ---------------------------------------------------------------------------
# Domain registry
# ---------------------------------------------------------------------------
# mode: "diff" (key-match + value diff) | "coverage" (counts only, shapes not
# row-comparable). table is queried via _query_supabase; fallback names the
# supabase_data._fallback_* helper.
_DOMAINS: List[Dict[str, Any]] = [
    {
        "name": "channel_benchmarks",
        "table": "channel_benchmarks",
        "params": "select=*&limit=1000",
        "fallback": "_fallback_channel_benchmarks",
        "fallback_args": ("", ""),
        "keys": ["channel", "industry"],
        "compare": ["cpc", "cpa", "pricing_model"],
        "mode": "diff",
    },
    {
        "name": "market_trends",
        "table": "market_trends",
        "params": "select=*&limit=1000",
        "fallback": "_fallback_market_trends",
        "fallback_args": ("", 1000),
        "keys": ["title"],
        "compare": ["category", "source"],
        "mode": "diff",
    },
    {
        "name": "salary_data",
        "table": "salary_data",
        "params": "select=*&limit=1000",
        "fallback": "_fallback_salary_data",
        "fallback_args": ("", ""),
        "keys": ["role", "location"],
        "compare": [],
        "mode": "diff",
    },
    {
        "name": "compliance_rules",
        "table": "compliance_rules",
        "params": "select=*&limit=1000",
        "fallback": "_fallback_compliance_rules",
        "fallback_args": ("", ""),
        "keys": ["rule_type", "jurisdiction"],
        "compare": [],
        "mode": "diff",
    },
    {
        "name": "vendor_profiles",
        "table": "vendor_profiles",
        "params": "select=*&limit=1000",
        "fallback": "_fallback_vendor_profiles",
        "fallback_args": ("",),
        "keys": ["name"],
        "compare": [],
        "mode": "diff",
    },
    {
        "name": "supply_repository",
        "table": "supply_repository",
        "params": "select=*&limit=1000",
        "fallback": "_fallback_supply_repository",
        "fallback_args": ("", ""),
        "keys": ["name"],
        "compare": [],
        "mode": "coverage",
    },
]


def _fallback_rows(spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    fn = getattr(sd, spec["fallback"], None)
    if not callable(fn):
        return []
    try:
        rows = fn(*spec.get("fallback_args", ()))
    except Exception:  # noqa: BLE001 -- audit must never raise
        return []
    return rows if isinstance(rows, list) else []


def audit_domain(name: str) -> Dict[str, Any]:
    """Audit one domain. Returns a structured report (never raises)."""
    spec = next((d for d in _DOMAINS if d["name"] == name), None)
    if spec is None:
        return {"domain": name, "error": "unknown domain", "verdict": "error"}

    try:
        sup_rows = sd._query_supabase(spec["table"], spec["params"])
    except Exception as exc:  # noqa: BLE001
        sup_rows = []
        sup_err = str(exc)
    else:
        sup_err = ""
    if not isinstance(sup_rows, list):
        sup_rows = []
    json_rows = _fallback_rows(spec)

    if spec["mode"] == "coverage":
        report = {
            "supabase_count": len(sup_rows),
            "json_count": len(json_rows),
            "matched": 0,
            "only_supabase": 0,
            "only_json": 0,
            "value_mismatches": 0,
            "note": "coverage-only: fallback shape is not row-comparable",
        }
    else:
        report = diff_rows(sup_rows, json_rows, spec["keys"], spec["compare"])

    report["domain"] = name
    report["mode"] = spec["mode"]
    report["match_keys"] = spec["keys"]
    if sup_err:
        report["supabase_error"] = sup_err[:200]
    report["verdict"] = "coverage" if spec["mode"] == "coverage" else _verdict(report)
    if report["verdict"] == "supabase_only":
        report["warning"] = (
            "No JSON fallback for this domain -- Supabase is a single point of "
            "failure. Seed a curated JSON fallback or confirm Supabase coverage."
        )
    return report


def run_parity_audit(domains: Optional[List[str]] = None) -> Dict[str, Any]:
    """Audit all (or the named) domains and return an aggregate report.

    READ-ONLY. Does not change serving behaviour. Includes a Supabase-enabled
    flag so callers know whether the Supabase half is even reachable.
    """
    names = domains or [d["name"] for d in _DOMAINS]
    results = [audit_domain(n) for n in names]
    verdicts: Dict[str, int] = {}
    for r in results:
        verdicts[r.get("verdict", "error")] = verdicts.get(r.get("verdict", "error"), 0) + 1

    # Cutover readiness: every diffable domain in parity, and the supabase-only
    # domains acknowledged (they don't block, but they're flagged).
    diffable = [r for r in results if r.get("mode") == "diff" and r.get("verdict") != "supabase_only"]
    ready = bool(diffable) and all(r.get("verdict") == "in_parity" for r in diffable)

    return {
        "supabase_enabled": bool(getattr(sd, "_ENABLED", False)),
        "domains_audited": len(results),
        "verdict_counts": verdicts,
        "cutover_ready": ready,
        "supabase_only_domains": [
            r["domain"] for r in results if r.get("verdict") == "supabase_only"
        ],
        "domains": results,
    }
