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
}

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

        kb: dict[str, Any] = {}
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
        _validate_freshness(kb)

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
        _validate_freshness(kb_updated)

        # Atomic swap of the entire KB dict reference
        _knowledge_base = kb_updated


def _reload_loop() -> None:
    """Background loop: sleep, then check for file changes.

    Runs forever on a daemon thread so it doesn't prevent process exit.
    """
    while True:
        time.sleep(KB_RELOAD_INTERVAL_SECONDS)
        try:
            _check_and_reload()
        except Exception as e:
            logger.error("KB hot-reload loop error: %s", e, exc_info=True)


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
