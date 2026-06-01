#!/usr/bin/env python3
"""refresh_market_data.py -- refresh data/live_market_data.json from verified sources.

Replaces the dead Firecrawl scrape path (removed in S72) with a re-runnable
refresh that reconciles the live-market overlay against the authoritative,
high-reliability in-repo benchmark file and (optionally) live Adzuna salary
data.

WHY THIS EXISTS
---------------
`benchmark_registry.py` overlays `data/live_market_data.json` on top of its
static CHANNEL_BENCHMARKS, reading:
  - job_boards.<board>.avg_cpc_typical | avg_cpc        (per-channel CPC)
  - industry_benchmarks.<industry>.avg_cost_per_hire    (per-industry CPH)
  - industry_benchmarks.<industry>.avg_cpa / avg_cpc / apply_rate_pct
The overlay had gone stale (last scraped 2026-04-05) with no refresh mechanism
after Firecrawl was removed. This script restores a mechanism sourced ONLY from
verified data:
  1. data/recruitment_benchmarks_comprehensive_2026.json
     -> A_cpa_cph_benchmarks_by_channel (reliability: "high", SHRM/Appcast 2025-26)
  2. (optional) live Adzuna API for real-time salary context

DESIGN PRINCIPLES
-----------------
- UPDATE-IN-PLACE: never drops existing overlay structure; only refreshes the
  numeric fields that have a verified source. No data loss.
- NO FABRICATION: every written number traces to a cited in-repo source or a
  live API response. Fields without a verified source are left untouched.
- HONEST DATING: writes a `refresh_log` recording what was refreshed, from which
  source, and when (timestamp passed in via --now so the script stays
  deterministic / reproducible).
- IDEMPOTENT: re-running with the same inputs yields the same output.

USAGE
-----
    python3 scripts/refresh_market_data.py --now 2026-06-02 --dry-run
    python3 scripts/refresh_market_data.py --now 2026-06-02
    python3 scripts/refresh_market_data.py --now 2026-06-02 --with-adzuna
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("refresh_market_data")

_ROOT = Path(__file__).resolve().parent.parent
_OVERLAY_PATH = _ROOT / "data" / "live_market_data.json"
_SOURCE_PATH = _ROOT / "data" / "recruitment_benchmarks_comprehensive_2026.json"

# Map the source's platform labels -> overlay job_board keys. Only the six
# boards the overlay/registry already know are updated; vertical-specific
# variants like "Indeed (Healthcare)" are ignored for the board-level CPC.
_PLATFORM_TO_BOARD: dict[str, str] = {
    "indeed": "indeed",
    "linkedin": "linkedin",
    "ziprecruiter": "ziprecruiter",
    "glassdoor": "glassdoor",
    "monster": "monster",
    "careerbuilder": "careerbuilder",
}

# Map the source's industry labels -> overlay industry_benchmarks keys.
_INDUSTRY_TO_KEY: dict[str, str] = {
    "healthcare / nursing": "healthcare",
    "healthcare / life sciences": "healthcare",
    "technology / it": "technology",
    "technology / engineering": "engineering",
    "finance / insurance": "finance",
    "retail / hospitality": "retail",
    "manufacturing": "manufacturing",
    "logistics / transportation": "manufacturing",
    "hospitality": "hospitality",
}


def _load_json(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# Upper bound for a plausible per-CLICK cost. Job-board CPC tops out around
# LinkedIn sponsored ($12); anything higher is a posting/slot/CPA price
# mislabelled as CPC (e.g. Glassdoor's $150 = 30-day sponsored post, not a
# click). Such values are rejected so they never reach budget math.
_MAX_PLAUSIBLE_CPC = 15.0


def _plausible_cpc(v: Any) -> bool:
    return (
        isinstance(v, (int, float))
        and not isinstance(v, bool)
        and 0 < v <= _MAX_PLAUSIBLE_CPC
    )


def _platform_cpc_map(source_a: dict[str, Any]) -> dict[str, dict[str, float]]:
    """Extract {board_key: {min,max,typical}} from cpc_by_platform.data.

    A board is only written if a plausible ``avg_cpc_typical`` can be produced
    (real median, or midpoint of a plausible min/max). Boards whose source
    "CPC" is actually slot/posting pricing (Glassdoor, ZipRecruiter,
    CareerBuilder) are skipped -> benchmark_registry keeps its static value.
    """
    out: dict[str, dict[str, float]] = {}
    rows = ((source_a.get("cpc_by_platform") or {}).get("data")) or []
    for row in rows:
        if not isinstance(row, dict):
            continue
        plat = str(row.get("platform") or "").strip().lower()
        board = _PLATFORM_TO_BOARD.get(plat)
        if not board:
            continue  # skip vertical variants / unknown boards
        cpc_min = row.get("cpc_min_usd")
        cpc_max = row.get("cpc_max_usd")
        cpc_med = row.get("cpc_median_usd")
        # Derive a typical only from plausible inputs.
        typical: float | None = None
        if _plausible_cpc(cpc_med):
            typical = float(cpc_med)
        elif _plausible_cpc(cpc_min) and _plausible_cpc(cpc_max):
            typical = round((float(cpc_min) + float(cpc_max)) / 2.0, 2)
        if typical is None:
            logger.info(
                "skip %s CPC (no plausible click cost: min=%s max=%s med=%s)",
                board,
                cpc_min,
                cpc_max,
                cpc_med,
            )
            continue
        entry: dict[str, float] = {"avg_cpc_typical": typical}
        if _plausible_cpc(cpc_min):
            entry["avg_cpc_min"] = float(cpc_min)
        if _plausible_cpc(cpc_max):
            entry["avg_cpc_max"] = float(cpc_max)
        out[board] = entry
    return out


def _industry_map(source_a: dict[str, Any]) -> dict[str, dict[str, float]]:
    """Extract {industry_key: {avg_cpa, apply_rate_pct, avg_cost_per_hire}}."""
    out: dict[str, dict[str, float]] = {}

    def _key(label: str) -> str | None:
        return _INDUSTRY_TO_KEY.get(str(label or "").strip().lower())

    for row in ((source_a.get("cpa_by_industry_vertical") or {}).get("data")) or []:
        if not isinstance(row, dict):
            continue
        k = _key(row.get("industry"))
        if not k:
            continue
        rng = row.get("cpa_range_usd")
        bucket = out.setdefault(k, {})
        if isinstance(rng, dict):
            lo, hi = rng.get("min"), rng.get("max")
            if isinstance(lo, (int, float)) and isinstance(hi, (int, float)):
                bucket["avg_cpa"] = round((lo + hi) / 2.0, 2)
        ar = row.get("apply_rate_pct")
        if isinstance(ar, (int, float)):
            bucket["apply_rate_pct"] = float(ar)

    for row in ((source_a.get("cph_by_industry") or {}).get("data")) or []:
        if not isinstance(row, dict):
            continue
        k = _key(row.get("industry"))
        if not k:
            continue
        # Prefer the midpoint of range_usd (more representative) over the
        # single cph_usd floor. Healthcare: {5000..12000} -> 8500, not 5000.
        cph_val: float | None = None
        rng = row.get("range_usd")
        if isinstance(rng, dict):
            lo, hi = rng.get("min"), rng.get("max")
            if isinstance(lo, (int, float)) and isinstance(hi, (int, float)):
                cph_val = round((lo + hi) / 2.0)
        if cph_val is None:
            cph = row.get("cph_usd")
            if isinstance(cph, (int, float)):
                cph_val = float(cph)
        if cph_val is not None:
            out.setdefault(k, {})["avg_cost_per_hire"] = cph_val
    return out


def _maybe_adzuna_salary() -> dict[str, Any] | None:
    """Optional: pull a small live Adzuna salary sample (verified, fresh).

    Returns None if the module/creds are unavailable. Never raises.
    """
    try:
        import api_integrations  # noqa: F401
        from api_integrations import get_api_integrations  # type: ignore
    except Exception:
        logger.warning("api_integrations unavailable -- skipping Adzuna sample")
        return None
    try:
        api = get_api_integrations()
        sample: dict[str, Any] = {}
        for role in ("registered nurse", "software engineer", "warehouse associate"):
            try:
                stats = api.get_salary_stats(role)  # type: ignore[attr-defined]
                if isinstance(stats, dict) and stats:
                    sample[role] = stats
            except Exception as exc:  # pragma: no cover
                logger.warning("Adzuna salary for %r failed: %s", role, exc)
        return sample or None
    except Exception as exc:  # pragma: no cover
        logger.warning("Adzuna init failed: %s", exc)
        return None


def refresh(now: str, with_adzuna: bool, dry_run: bool) -> int:
    """Refresh the overlay. Returns process exit code (0 = success)."""
    if not _OVERLAY_PATH.exists():
        logger.error("overlay not found: %s", _OVERLAY_PATH)
        return 1
    if not _SOURCE_PATH.exists():
        logger.error("verified source not found: %s", _SOURCE_PATH)
        return 1

    overlay = _load_json(_OVERLAY_PATH)
    source = _load_json(_SOURCE_PATH)
    source_a = source.get("A_cpa_cph_benchmarks_by_channel") or {}
    if not source_a:
        logger.error("source A_cpa_cph_benchmarks_by_channel missing/empty")
        return 1

    changes: list[str] = []

    # 1) Per-board CPC
    board_cpc = _platform_cpc_map(source_a)
    job_boards = overlay.setdefault("job_boards", {})
    for board, fields in board_cpc.items():
        jb = job_boards.setdefault(board, {})
        for fk, fv in fields.items():
            if jb.get(fk) != fv:
                changes.append(f"job_boards.{board}.{fk}: {jb.get(fk)} -> {fv}")
                jb[fk] = fv

    # 2) Per-industry CPA / CPH / apply rate
    ind_map = _industry_map(source_a)
    ind_bench = overlay.setdefault("industry_benchmarks", {})
    for ind, fields in ind_map.items():
        ib = ind_bench.setdefault(ind, {})
        for fk, fv in fields.items():
            if ib.get(fk) != fv:
                changes.append(f"industry_benchmarks.{ind}.{fk}: {ib.get(fk)} -> {fv}")
                ib[fk] = fv

    # 3) Optional live Adzuna salary sample (additive, source-tagged)
    adzuna_sample = _maybe_adzuna_salary() if with_adzuna else None
    if adzuna_sample:
        overlay["live_salary_adzuna"] = {
            "retrieved": now,
            "source": "Adzuna API (live)",
            "roles": adzuna_sample,
        }
        changes.append(f"live_salary_adzuna: {len(adzuna_sample)} roles")

    # 4) Honest provenance
    overlay["scraped_at"] = now
    overlay["data_freshness"] = (
        f"Reconciled {now} from recruitment_benchmarks_comprehensive_2026 "
        f"(reliability: high)" + (" + live Adzuna" if adzuna_sample else "")
    )
    overlay["refresh_log"] = {
        "refreshed_at": now,
        "method": "refresh_market_data.py (reconcile vs verified in-repo source)",
        "primary_source": "recruitment_benchmarks_comprehensive_2026.json"
        " -> A_cpa_cph_benchmarks_by_channel",
        "adzuna_live": bool(adzuna_sample),
        "fields_changed": len(changes),
    }

    logger.info("computed %d field change(s)", len(changes))
    for c in changes[:40]:
        logger.info("  %s", c)
    if len(changes) > 40:
        logger.info("  ... and %d more", len(changes) - 40)

    if dry_run:
        logger.info("DRY-RUN: not writing %s", _OVERLAY_PATH.name)
        return 0

    with open(_OVERLAY_PATH, "w", encoding="utf-8") as f:
        json.dump(overlay, f, indent=2, ensure_ascii=False)
    logger.info("wrote %s (%d field changes)", _OVERLAY_PATH.name, len(changes))
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Refresh live_market_data.json")
    p.add_argument(
        "--now",
        required=True,
        help="Refresh date stamp, YYYY-MM-DD (kept explicit for reproducibility)",
    )
    p.add_argument("--with-adzuna", action="store_true", help="Pull live Adzuna salary")
    p.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = p.parse_args()
    return refresh(args.now, args.with_adzuna, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
