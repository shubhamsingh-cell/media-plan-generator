"""Standalone knowledge base loader with hot-reload support.

Extracted from app.py to avoid circular imports when ppt_generator.py or
data_orchestrator.py need to load the KB as a fallback.

Hot-reload: A background daemon thread checks file modification times every
5 minutes.  When a data/ JSON file has been modified since last load, only
that file is re-read and swapped into the in-memory KB under the lock.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("kb_loader")

_knowledge_base: dict[str, Any] | None = None
_kb_lock = threading.Lock()

# Tracks last-seen mtime (epoch float) per filename so the reload thread
# can detect which files changed without re-reading every file.
_file_mtimes: dict[str, float] = {}

# ═══════════════════════════════════════════════════════════════════════════════
# REQUEST-SCOPED READ TRACKING (S56 P0 -- "Transparency Panel Lie" fix)
# ═══════════════════════════════════════════════════════════════════════════════
#
# The "Why this answer?" panel used to populate ``kb_files_queried`` from a
# static label map (``_SOURCE_TO_KB_FILES``) in nova.py. That map fabricated
# attribution -- it claimed KB files were read when the request path never
# opened them. This module now records the set of top-level KB keys actually
# accessed during the current chat turn via ``kb.get(...)`` / ``kb[...]``.
#
# API:
#   start_request_tracking() -> None       # init thread-local set for this turn
#   record_read(key: str)    -> None       # called on every kb dict access
#   get_read_keys()          -> set[str]   # snapshot of keys read this turn
#   stop_request_tracking()  -> None       # clear thread-local
#
# Usage pattern (in handle_chat_request):
#     kb_loader.start_request_tracking()
#     try:
#         ... run the turn ...
#         read_keys = kb_loader.get_read_keys()
#         kb_files_queried = kb_loader.keys_to_filenames(read_keys)
#     finally:
#         kb_loader.stop_request_tracking()
#
# Thread-safety: each request runs on its own thread (WSGI worker / SSE thread),
# so ``threading.local()`` gives us automatic per-request isolation without
# locks. ``set.add`` is O(1) and atomic, so tracking adds no measurable
# latency. If ``start_request_tracking`` was never called on this thread,
# ``record_read`` is a no-op (graceful degradation).

_tracking_state = threading.local()


def start_request_tracking() -> None:
    """Begin recording KB reads for the current thread / request.

    Call this at the top of each chat-request handler. Subsequent
    ``record_read(key)`` calls will populate a per-thread set until
    ``stop_request_tracking()`` is called.
    """
    _tracking_state.keys = set()


# Public-alias API matching the spec names requested by the transparency
# panel fix. These are thin wrappers so callers can write
# ``kb_loader.start_tracking()`` / ``kb_loader.get_tracked_reads()`` without
# having to remember the slightly longer internal names.
def start_tracking() -> None:
    """Alias for ``start_request_tracking`` -- clears any prior read set."""
    start_request_tracking()


def record_read(key: Any) -> None:
    """Record that ``key`` was read from the KB on this thread.

    Safe to call without prior ``start_request_tracking()`` -- it becomes a
    no-op so background jobs, startup code, and tests never raise.

    Args:
        key: A top-level KB key (e.g. ``"joveo_publishers"``,
            ``"salary_benchmarks_detailed"``). Non-string keys and keys
            starting with ``_`` (internal bookkeeping like
            ``_freshness_warnings``) are filtered out.
    """
    if not isinstance(key, str) or not key or key.startswith("_"):
        return
    keys = getattr(_tracking_state, "keys", None)
    if keys is None:
        return  # Tracking not started on this thread -- no-op.
    keys.add(key)


def get_read_keys() -> set[str]:
    """Return a snapshot of keys read on this thread since tracking started.

    Returns an empty set if tracking was never started.
    """
    keys = getattr(_tracking_state, "keys", None)
    if keys is None:
        return set()
    return set(keys)  # defensive copy


def get_tracked_reads() -> set[str]:
    """Alias for ``get_read_keys`` -- returns empty set if tracking off."""
    return get_read_keys()


def stop_request_tracking() -> None:
    """Clear the thread-local tracking set.

    Call this in a ``finally:`` block so it always runs, even on exception.
    After this returns, ``record_read`` calls on this thread become no-ops
    again until the next ``start_request_tracking()``.
    """
    try:
        del _tracking_state.keys
    except AttributeError:
        pass


def stop_tracking() -> None:
    """Alias for ``stop_request_tracking``."""
    stop_request_tracking()


def keys_to_filenames(keys: set[str] | list[str]) -> list[str]:
    """Map tracked KB keys to their on-disk filenames.

    Resolves through ``KB_ALIASES`` first so legacy callers that hit an
    alias key (e.g. ``"knowledge_base"``) report the primary filename
    (``recruitment_industry_knowledge.json``) instead of a missing key.

    Args:
        keys: Iterable of KB keys captured by ``get_tracked_reads``.

    Returns:
        Sorted, de-duplicated list of KB filenames. Unknown / derived keys
        (e.g. backward-compat leaves like ``"salary_trends"`` that come from
        inside ``"core"``) are silently dropped so the panel only shows
        physical files that genuinely back the answer.
    """
    if not keys:
        return []
    out: set[str] = set()
    for k in keys:
        if not isinstance(k, str) or not k or k.startswith("_"):
            continue
        primary = KB_ALIASES.get(k, k)
        filename = KB_FILES.get(primary)
        if filename:
            out.add(filename)
    return sorted(out)


# ═══════════════════════════════════════════════════════════════════════════════
# TRACKED DICT -- auto-records every read into the request-scoped set
# ═══════════════════════════════════════════════════════════════════════════════
#
# Nova's ``self._data_cache`` is just a reference to the singleton returned
# by ``load_knowledge_base()``. Making that singleton a ``TrackedDict``
# means every existing ``self._data_cache.get("joveo_publishers")`` and
# ``self._data_cache["channels_db"]`` call site automatically feeds the
# tracker -- zero callsite changes, zero drift risk.
#
# ``record_read`` is a no-op when tracking wasn't started on the thread, so
# startup code (``_rebuild_backward_compat``, ``_apply_aliases``,
# ``_validate_freshness``) doesn't pollute any request's read set.
class TrackedDict(dict):
    """``dict`` subclass that feeds every read into ``record_read``.

    Overrides only the read paths used by the codebase today (``d[k]``,
    ``d.get(k)``, ``k in d``). Writes are left alone. Unknown keys still
    raise ``KeyError`` exactly like a normal dict -- we record the attempt
    *before* the lookup fails so the transparency panel can show "Nova
    looked for X but the file wasn't loaded". Internal bookkeeping keys
    (``_freshness_warnings``, ``_nova_client_plans_merged``, etc.) are
    filtered out by ``record_read`` itself.
    """

    __slots__ = ()

    def __getitem__(self, key: Any) -> Any:
        record_read(key)
        return super().__getitem__(key)

    def get(self, key: Any, default: Any = None) -> Any:  # type: ignore[override]
        record_read(key)
        return super().get(key, default)

    def __contains__(self, key: Any) -> bool:  # type: ignore[override]
        record_read(key)
        return super().__contains__(key)


# Set to True once the reload thread has been started (prevents duplicates).
_reload_thread_started: bool = False

KB_FILES: dict[str, str] = {
    "core": "recruitment_industry_knowledge.json",
    "platform_intelligence": "platform_intelligence_deep.json",
    "recruitment_benchmarks": "recruitment_benchmarks_deep.json",
    "recruitment_strategy": "recruitment_strategy_intelligence.json",
    "regional_hiring": "regional_hiring_intelligence.json",
    "supply_ecosystem": "supply_ecosystem_intelligence.json",
    "workforce_trends": "workforce_trends_intelligence.json",
    "white_papers": "industry_white_papers.json",
    "joveo_2026_benchmarks": "joveo_2026_benchmarks.json",
    "google_ads_benchmarks": "google_ads_2025_benchmarks.json",
    "external_benchmarks": "external_benchmarks_2025.json",
    "client_media_plans": "client_media_plans_kb.json",
    "international_sources": "international_sources.json",
    # 2026 Research Data (added 2026-03-26)
    "hr_tech_landscape": "hr_tech_landscape_2026.json",
    "publisher_benchmarks": "publisher_benchmarks_2026.json",
    "recruitment_marketing_trends": "recruitment_marketing_trends_2026.json",
    "labor_market_outlook": "labor_market_outlook_2026.json",
    "salary_benchmarks_detailed": "salary_benchmarks_detailed_2026.json",
    "ad_benchmarks_recruitment": "ad_benchmarks_recruitment_2026.json",
    "industry_hiring_patterns": "industry_hiring_patterns_2026.json",
    "top_employers_by_city": "top_employers_by_city_2026.json",
    "compliance_regulations": "compliance_regulations_2026.json",
    "agency_rpo_market": "agency_rpo_market_2026.json",
    # S30: Global supply + client plans for vector search indexing
    "global_supply_repository": "joveo_global_supply_repository.json",
    "rtx_media_plan": "client_plans/rtx_usa_media_plan.json",
    "rtx_aerospace_benchmarks": "client_plans/rtx_aerospace_defense_benchmarks.json",
    # S30: Joveo JAX CPA benchmarks (304 categories, real programmatic data)
    "joveo_cpa_benchmarks": "joveo_cpa_benchmarks_2026.json",
    # S48: Real channel performance benchmarks (SlotOps 108K + CG 98K)
    "craigslist_benchmarks": "craigslist_performance_benchmarks.json",
    "linkedin_benchmarks": "linkedin_performance_benchmarks.json",
    # S50: 15 previously unindexed data files
    "adzuna_benchmarks": "adzuna_benchmarks.json",
    "channel_benchmarks_live": "channel_benchmarks_live.json",
    "channels_db": "channels_db.json",
    "competitor_careers": "competitor_careers.json",
    "fred_indicators": "fred_indicators.json",
    "google_trends": "google_trends.json",
    "h1b_salary_intelligence": "h1b_salary_intelligence.json",
    "job_density_metros": "job_density_metros.json",
    "job_posting_volumes": "job_posting_volumes.json",
    "joveo_publishers": "joveo_publishers.json",
    "live_market_data": "live_market_data.json",
    "market_trends_live": "market_trends_live.json",
    "platform_ad_specs": "platform_ad_specs.json",
    "seasonal_hiring_trends": "seasonal_hiring_trends.json",
    "global_supply": "global_supply.json",
    # S52: Healthcare supply map (US) -- 350 partners across 64 categories,
    # merged from Claude-authored audit (master map + comprehensive partners
    # + recommendations + gap analysis). Drives the fast-path for healthcare
    # listing queries so Nova matches Claude.ai chat-quality for supply
    # partner lookups.
    "healthcare_supply_map_us": "healthcare_supply_map_us.json",
    # S52: Derived healthcare indexes built from the same 3 source xlsx files
    # as healthcare_supply_map_us. Each serves a different product query
    # pattern:
    #   - partner_specialty_crosswalk: "what partners cover RNs/cardiology/PT?"
    #   - partner_url_registry:        O(1) lookup by normalized URL key
    #   - category_to_partners:        "list all diversity-focused partners"
    "partner_specialty_crosswalk": "partner_specialty_crosswalk.json",
    "partner_url_registry": "partner_url_registry.json",
    "category_to_partners": "category_to_partners.json",
    # S54: Web-researched authoritative benchmark KBs (47 distinct sources:
    # Appcast 2024, BLS OES, Medscape, SHRM, LinkedIn Talent Insights, JOLTS,
    # Joveo 2026 internal, C2ER COLI, Merritt Hawkins, Nurse.com salary
    # reports). Covers 12 verticals x 40 US metros x 7 channels + 100 top
    # employers + 49 healthcare roles. Reusable across Nova chat, media plan
    # generator, and all Plan/Intelligence/Compliance products.
    "recruitment_benchmarks_2026_deep": "recruitment_benchmarks_2026_deep.json",
    "employer_career_intelligence_2026": "employer_career_intelligence_2026.json",
    "healthcare_specialty_pay_2026": "healthcare_specialty_pay_2026.json",
    # S56 unification: files previously loaded only by nova._data_cache.
    # Registered here so the central loader, hot-reload watcher, freshness
    # checker, and transparency tooling all cover them too.
    "linkedin_guidewire": "linkedin_guidewire_data.json",
    "international_benchmarks": "international_benchmarks_2026.json",
}

# ─────────────────────────────────────────────────────────────────────────────
# S56 unification: alias map for callers that historically used different
# keys on nova._data_cache than kb_loader uses.  Keeping aliases here (rather
# than forcing callers to change) makes the change a pure additive fix with
# zero risk to existing tools.
#
# Reads on the alias key (e.g. ``kb["knowledge_base"]``) resolve to the
# primary KB_FILES entry (e.g. ``kb["core"]``).
# ─────────────────────────────────────────────────────────────────────────────
KB_ALIASES: dict[str, str] = {
    # nova._data_cache["knowledge_base"] loaded recruitment_industry_knowledge.json,
    # which kb_loader already loads under the "core" key.
    "knowledge_base": "core",
    # nova._data_cache["expanded_supply_repo"] loaded joveo_global_supply_repository.json,
    # which kb_loader already loads under the "global_supply_repository" key.
    "expanded_supply_repo": "global_supply_repository",
}

# Maximum file age (in days) before a startup warning is logged.
# Lowered from 180 to 90 days (2026-04-07) to catch stale benchmarks sooner.
_FILE_FRESHNESS_THRESHOLD_DAYS: int = 90

# How often the reload thread checks for file changes (seconds).
KB_RELOAD_INTERVAL_SECONDS: int = 300  # 5 minutes

_DATA_DIR: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def _get_file_mtime(filepath: str) -> float:
    """Return file modification time as epoch float, or 0.0 if missing."""
    try:
        return os.path.getmtime(filepath)
    except OSError:
        return 0.0


def _rebuild_backward_compat(kb: dict[str, Any]) -> None:
    """Merge core section keys into top level for backward compatibility.

    Existing code references ``kb["benchmarks"]``, ``kb["salary_trends"]``,
    etc.  This copies those keys from ``kb["core"]`` into the top level
    without overwriting section keys.
    """
    core = kb.get("core", {})
    for k, v in core.items():
        if k not in kb:
            kb[k] = v


def _apply_aliases(kb: dict[str, Any]) -> None:
    """Populate ``KB_ALIASES`` keys so legacy callers keep working.

    Each alias points to the same in-memory object as its primary key so
    hot-reloads (which swap section values inside ``_knowledge_base``) remain
    visible through the alias name.  Aliases never overwrite an existing
    top-level key -- if a caller somehow wrote to an alias directly, we
    respect that.
    """
    for alias, primary in KB_ALIASES.items():
        if alias in kb:
            continue
        if primary in kb:
            kb[alias] = kb[primary]


def _validate_freshness(kb: dict[str, Any]) -> None:
    """Check last_updated metadata and add warnings for stale sections."""
    stale_sections: list[tuple[str, str, int]] = []
    try:
        today = datetime.datetime.now()
        max_age_days = 90
        for section_key, section_data in kb.items():
            if not isinstance(section_data, dict):
                continue
            last_updated_str: str | None = None
            if isinstance(section_data.get("metadata"), dict):
                last_updated_str = section_data["metadata"].get("last_updated")
            if not last_updated_str:
                last_updated_str = section_data.get("last_updated")
            if last_updated_str and isinstance(last_updated_str, str):
                try:
                    lu_date = datetime.datetime.strptime(
                        last_updated_str[:10], "%Y-%m-%d"
                    )
                    age_days = (today - lu_date).days
                    if age_days > max_age_days:
                        stale_sections.append((section_key, last_updated_str, age_days))
                except (ValueError, TypeError):
                    pass
        if stale_sections:
            for skey, sdate, sage in stale_sections:
                logger.warning(
                    "KB DATA FRESHNESS WARNING: '%s' last updated %s "
                    "(%d days ago, threshold=%d days)",
                    skey,
                    sdate,
                    sage,
                    max_age_days,
                )
            kb["_freshness_warnings"] = [
                {"section": s, "last_updated": d, "age_days": a}
                for s, d, a in stale_sections
            ]
    except Exception as e:
        logger.warning("KB freshness check failed (non-fatal): %s", e)


def _check_file_freshness_at_startup() -> list[dict[str, Any]]:
    """Check on-disk file ages and warn if any data file exceeds the freshness threshold.

    Scans all files in KB_FILES and reports those whose filesystem modification
    time is older than ``_FILE_FRESHNESS_THRESHOLD_DAYS``.  Called once during
    the initial ``load_knowledge_base()`` call.

    Returns:
        List of dicts with keys: filename, section_key, age_days, mtime_iso.
    """
    stale: list[dict[str, Any]] = []
    now = datetime.datetime.now()
    for section_key, filename in KB_FILES.items():
        fpath = os.path.join(_DATA_DIR, filename)
        mtime = _get_file_mtime(fpath)
        if mtime <= 0:
            continue
        mtime_dt = datetime.datetime.fromtimestamp(mtime)
        age_days = (now - mtime_dt).days
        if age_days > _FILE_FRESHNESS_THRESHOLD_DAYS:
            stale.append(
                {
                    "filename": filename,
                    "section_key": section_key,
                    "age_days": age_days,
                    "mtime_iso": mtime_dt.isoformat(),
                }
            )
            logger.warning(
                "DATA FRESHNESS: '%s' (%s) is %d days old (threshold=%d days). "
                "Consider refreshing this data file.",
                filename,
                section_key,
                age_days,
                _FILE_FRESHNESS_THRESHOLD_DAYS,
            )
    return stale


def get_data_freshness_report() -> dict[str, Any]:
    """Build a freshness report for every file in the data/ directory.

    Designed to be called by the ``/api/data/freshness`` endpoint. Reports the
    age, last-modified timestamp, and staleness status for each KB file plus
    all supplementary JSON files found in the data directory.

    Returns:
        Dict with ``files`` list, ``stale_count``, and ``checked_at`` ISO timestamp.
    """
    now = datetime.datetime.now()
    files_report: list[dict[str, Any]] = []

    # KB_FILES (primary knowledge base files)
    for section_key, filename in KB_FILES.items():
        fpath = os.path.join(_DATA_DIR, filename)
        mtime = _get_file_mtime(fpath)
        if mtime <= 0:
            files_report.append(
                {
                    "filename": filename,
                    "section_key": section_key,
                    "exists": False,
                    "age_days": None,
                    "mtime_iso": None,
                    "stale": True,
                    "source": "kb_primary",
                }
            )
            continue
        mtime_dt = datetime.datetime.fromtimestamp(mtime)
        age_days = (now - mtime_dt).days
        files_report.append(
            {
                "filename": filename,
                "section_key": section_key,
                "exists": True,
                "age_days": age_days,
                "mtime_iso": mtime_dt.isoformat(),
                "stale": age_days > _FILE_FRESHNESS_THRESHOLD_DAYS,
                "source": "kb_primary",
            }
        )

    # Supplementary data files (*.json in data/ not already in KB_FILES)
    kb_filenames = set(KB_FILES.values())
    try:
        data_path = Path(_DATA_DIR)
        for json_file in sorted(data_path.glob("*.json")):
            if json_file.name in kb_filenames:
                continue
            mtime = _get_file_mtime(str(json_file))
            if mtime <= 0:
                continue
            mtime_dt = datetime.datetime.fromtimestamp(mtime)
            age_days = (now - mtime_dt).days
            files_report.append(
                {
                    "filename": json_file.name,
                    "section_key": None,
                    "exists": True,
                    "age_days": age_days,
                    "mtime_iso": mtime_dt.isoformat(),
                    "stale": age_days > _FILE_FRESHNESS_THRESHOLD_DAYS,
                    "source": "supplementary",
                }
            )
    except OSError as e:
        logger.warning("Failed to scan supplementary data files: %s", e)

    stale_count = sum(1 for f in files_report if f.get("stale"))
    return {
        "checked_at": now.isoformat(),
        "threshold_days": _FILE_FRESHNESS_THRESHOLD_DAYS,
        "total_files": len(files_report),
        "stale_count": stale_count,
        "files": files_report,
    }


def load_knowledge_base() -> dict[str, Any]:
    """Load and merge all knowledge base files into unified dict.

    Thread-safe, cached after first load.  Returns merged dict with section
    keys + backward-compat top-level keys, or a minimal dict on failure.

    After the initial load, starts a background daemon thread that checks
    for file modifications every 5 minutes and hot-reloads changed files.
    """
    global _knowledge_base
    with _kb_lock:
        if _knowledge_base is not None:
            return _knowledge_base

        # TrackedDict records every kb.get / kb[k] access into a per-request
        # thread-local set (see "REQUEST-SCOPED READ TRACKING" above). During
        # load the request tracker hasn't been started on this thread, so
        # all the setup reads below silently no-op through ``record_read``.
        kb: dict[str, Any] = TrackedDict()
        loaded_count = 0
        for section_key, filename in KB_FILES.items():
            fpath = os.path.join(_DATA_DIR, filename)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    kb[section_key] = json.load(f)
                    loaded_count += 1
                    logger.info("KB loaded %s (%s)", section_key, filename)
                    _file_mtimes[filename] = _get_file_mtime(fpath)
            except FileNotFoundError:
                kb[section_key] = {}
                logger.warning("KB file not found: %s", filename)
            except json.JSONDecodeError as e:
                kb[section_key] = {}
                logger.error("KB JSON error in %s: %s", filename, e)
            except OSError as e:
                kb[section_key] = {}
                logger.error("KB load error for %s: %s", filename, e)

        _rebuild_backward_compat(kb)
        _apply_aliases(kb)
        _validate_freshness(kb)

        # ── File-level freshness check (P3: data quality) ──
        try:
            stale_files = _check_file_freshness_at_startup()
            if stale_files:
                kb["_stale_data_files"] = stale_files
                logger.warning(
                    "DATA FRESHNESS: %d data file(s) exceed %d-day threshold",
                    len(stale_files),
                    _FILE_FRESHNESS_THRESHOLD_DAYS,
                )
        except Exception as freshness_err:
            logger.debug("File freshness check failed (non-fatal): %s", freshness_err)

        # ── KB Memory Usage Tracking (#14) ──
        try:
            kb_json_bytes = len(json.dumps(kb).encode("utf-8"))
            logger.info(
                "Knowledge base loaded: %d/%d files, %d total keys, ~%.1f MB in memory",
                loaded_count,
                len(KB_FILES),
                len(kb),
                kb_json_bytes / 1_048_576,
            )
            if kb_json_bytes > 50 * 1_048_576:  # warn above 50 MB
                logger.warning(
                    "KB memory usage HIGH: %.1f MB — consider lazy loading",
                    kb_json_bytes / 1_048_576,
                )
        except Exception as mem_err:
            logger.info(
                "Knowledge base loaded: %d/%d files, %d total keys (memory tracking failed: %s)",
                loaded_count,
                len(KB_FILES),
                len(kb),
                mem_err,
            )

        # ── KB Data Quality Validation (#16) ──
        try:
            quality_issues: list[dict[str, Any]] = []
            for qk, qv in kb.items():
                if qk.startswith("_"):
                    continue
                issues: list[str] = []
                if qv is None:
                    issues.append("null_data")
                elif isinstance(qv, dict) and len(qv) == 0:
                    issues.append("empty_dict")
                elif isinstance(qv, list) and len(qv) == 0:
                    issues.append("empty_list")
                if issues:
                    quality_issues.append(
                        {"key": qk, "issues": issues, "type": type(qv).__name__}
                    )
            if quality_issues:
                logger.warning(
                    "KB quality issues in %d sections: %s",
                    len(quality_issues),
                    ", ".join(
                        f"{qi['key']}({','.join(qi['issues'])})"
                        for qi in quality_issues
                    ),
                )
                kb["_quality_issues"] = quality_issues
            else:
                logger.info("KB data quality check passed: all sections have data")
        except Exception as qe:
            logger.debug("KB quality check failed (non-fatal): %s", qe)

        _knowledge_base = kb

    # Start the hot-reload daemon thread (once, outside the lock)
    _ensure_reload_thread()

    return _knowledge_base


# ═══════════════════════════════════════════════════════════════════════════════
# HOT-RELOAD BACKGROUND THREAD
# ═══════════════════════════════════════════════════════════════════════════════


def _check_and_reload() -> None:
    """Check all KB file mtimes; reload any that changed.

    Runs under _kb_lock so readers always see a consistent snapshot.
    Only files whose mtime is newer than the last recorded mtime are re-read.
    """
    global _knowledge_base
    if _knowledge_base is None:
        return  # Not yet loaded; nothing to reload

    changed_files: list[tuple[str, str]] = []  # (section_key, filename)

    # First pass: detect which files changed (no lock needed for os.stat)
    for section_key, filename in KB_FILES.items():
        fpath = os.path.join(_DATA_DIR, filename)
        current_mtime = _get_file_mtime(fpath)
        last_mtime = _file_mtimes.get(filename, 0.0)
        if current_mtime > 0 and current_mtime > last_mtime:
            changed_files.append((section_key, filename))

    if not changed_files:
        return

    # Second pass: reload changed files under the lock
    with _kb_lock:
        if _knowledge_base is None:
            return  # Race: someone cleared it

        # Work on a shallow copy so partial failures don't corrupt the KB
        kb_updated = dict(_knowledge_base)
        for section_key, filename in changed_files:
            fpath = os.path.join(_DATA_DIR, filename)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    kb_updated[section_key] = json.load(f)
                new_mtime = _get_file_mtime(fpath)
                _file_mtimes[filename] = new_mtime
                logger.info(f"KB hot-reload: refreshed {filename}")
            except FileNotFoundError:
                logger.warning(f"KB hot-reload: file disappeared: {filename}")
            except json.JSONDecodeError as e:
                logger.error(
                    f"KB hot-reload: JSON error in {filename}: {e}",
                    exc_info=True,
                )
                # Keep the old version for this section
            except OSError as e:
                logger.error(
                    f"KB hot-reload: read error for {filename}: {e}",
                    exc_info=True,
                )

        # Rebuild backward-compat keys and freshness after any section change
        _rebuild_backward_compat(kb_updated)
        _apply_aliases(kb_updated)
        _validate_freshness(kb_updated)

        # S56 unification: mutate the live dict in place rather than swapping
        # the module-level reference. External holders (e.g. Nova, which now
        # shares ``_data_cache`` with this singleton) see the updated values
        # without needing a fresh ``load_knowledge_base()`` call.
        #
        # The lock above guarantees readers see a consistent snapshot while
        # we swap values. Hot-reload only adds/updates keys; it never drops
        # them, so clearing first + updating is equivalent to a full rebuild
        # on the same dict object.
        live_kb = _knowledge_base  # non-None here (guarded above)
        if live_kb is not None:
            live_kb.clear()
            live_kb.update(kb_updated)


def _check_supabase_kb_freshness() -> None:
    """Check if Supabase knowledge_base table has newer data than in-memory KB.

    Queries the knowledge_base table for the most recent updated_at timestamp
    and compares against the last known sync time. If newer rows exist, fetches
    them and merges into the in-memory KB.

    Runs every 30 minutes as part of the hot-reload loop.
    """
    global _knowledge_base
    if _knowledge_base is None:
        return

    try:
        import ssl
        import urllib.request
        import urllib.parse

        _sb_url = os.environ.get("SUPABASE_URL") or ""
        _sb_key = (
            os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
            or os.environ.get("SUPABASE_ANON_KEY")
            or ""
        )
        if not _sb_url or not _sb_key:
            return

        base = _sb_url.rstrip("/")
        ssl_ctx = ssl.create_default_context()

        # Query for the max updated_at from knowledge_base
        # PostgREST: select=updated_at&order=updated_at.desc&limit=1
        select_param = urllib.parse.quote("category,key,data,updated_at", safe="")
        url = (
            f"{base}/rest/v1/knowledge_base"
            f"?select={select_param}"
            f"&order=updated_at.desc"
            f"&limit=50"
        )
        headers = {
            "apikey": _sb_key,
            "Authorization": f"Bearer {_sb_key}",
            "Accept": "application/json",
        }

        req = urllib.request.Request(url, method="GET", headers=headers)
        with urllib.request.urlopen(req, timeout=10, context=ssl_ctx) as resp:
            raw = resp.read().decode("utf-8")
            rows = json.loads(raw)

        if not isinstance(rows, list) or not rows:
            logger.debug("KB Supabase sync: no rows found in knowledge_base table")
            return

        # Check if any rows are newer than our last sync
        _last_sync_key = "_supabase_last_sync"
        last_sync = (_knowledge_base or {}).get(_last_sync_key) or ""

        new_rows = []
        for row in rows:
            row_updated = row.get("updated_at") or ""
            if row_updated > last_sync:
                new_rows.append(row)

        if not new_rows:
            logger.debug(
                "KB Supabase sync: no new data (last_sync=%s)",
                last_sync[:19] if last_sync else "never",
            )
            return

        # Merge new rows into the in-memory KB
        with _kb_lock:
            if _knowledge_base is None:
                return
            kb_updated = dict(_knowledge_base)
            merged_count = 0
            for row in new_rows:
                category = row.get("category") or ""
                key = row.get("key") or ""
                data = row.get("data")
                if not category or data is None:
                    continue

                # Store under category key in KB (create section if needed)
                section_key = f"supabase_{category}"
                if section_key not in kb_updated:
                    kb_updated[section_key] = {}
                if isinstance(kb_updated[section_key], dict):
                    kb_updated[section_key][key] = data
                    merged_count += 1

            # Update last sync timestamp
            newest_ts = max(r.get("updated_at") or "" for r in new_rows)
            kb_updated[_last_sync_key] = newest_ts

            _rebuild_backward_compat(kb_updated)
            _apply_aliases(kb_updated)
            # S56 unification: in-place update so external holders see the
            # Supabase merge without needing to re-fetch the KB reference.
            live_kb = _knowledge_base  # non-None here (guarded above)
            if live_kb is not None:
                live_kb.clear()
                live_kb.update(kb_updated)

        logger.info(
            "KB Supabase sync: merged %d new rows from knowledge_base table (newest=%s)",
            merged_count,
            newest_ts[:19] if newest_ts else "unknown",
        )

    except ImportError:
        logger.debug("KB Supabase sync: ssl/urllib not available")
    except (OSError, ValueError, KeyError, TypeError) as e:
        logger.warning("KB Supabase sync failed (non-fatal): %s", e)
    except Exception as e:
        logger.warning("KB Supabase sync unexpected error (non-fatal): %s", e)


# How often to check Supabase for KB updates (seconds).
KB_SUPABASE_SYNC_INTERVAL_SECONDS: int = 1800  # 30 minutes


def _reload_loop() -> None:
    """Background loop: sleep, then check for file changes and Supabase freshness.

    Runs forever on a daemon thread so it doesn't prevent process exit.
    Checks file mtimes every 5 minutes and Supabase every 30 minutes.
    """
    _supabase_check_counter = 0
    _supabase_checks_per_interval = (
        KB_SUPABASE_SYNC_INTERVAL_SECONDS // KB_RELOAD_INTERVAL_SECONDS
    )

    while True:
        time.sleep(KB_RELOAD_INTERVAL_SECONDS)

        # File mtime check (every 5 minutes)
        try:
            _check_and_reload()
        except Exception as e:
            logger.error("KB hot-reload loop error: %s", e, exc_info=True)

        # Supabase freshness check (every 30 minutes)
        _supabase_check_counter += 1
        if _supabase_check_counter >= _supabase_checks_per_interval:
            _supabase_check_counter = 0
            try:
                _check_supabase_kb_freshness()
            except Exception as e:
                logger.error("KB Supabase sync loop error: %s", e, exc_info=True)


def _ensure_reload_thread() -> None:
    """Start the hot-reload daemon thread exactly once."""
    global _reload_thread_started
    if _reload_thread_started:
        return
    _reload_thread_started = True
    t = threading.Thread(target=_reload_loop, name="kb-hot-reload", daemon=True)
    t.start()
    logger.info(
        "KB hot-reload thread started (interval=%ds)",
        KB_RELOAD_INTERVAL_SECONDS,
    )
