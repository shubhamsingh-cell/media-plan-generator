"""Standalone knowledge base loader.

Extracted from app.py to avoid circular imports when ppt_generator.py or
data_orchestrator.py need to load the KB as a fallback.
"""

import datetime
import json
import logging
import os
import threading

logger = logging.getLogger("kb_loader")

_knowledge_base = None
_kb_lock = threading.Lock()

KB_FILES = {
    "core":                   "recruitment_industry_knowledge.json",
    "platform_intelligence":  "platform_intelligence_deep.json",
    "recruitment_benchmarks": "recruitment_benchmarks_deep.json",
    "recruitment_strategy":   "recruitment_strategy_intelligence.json",
    "regional_hiring":        "regional_hiring_intelligence.json",
    "supply_ecosystem":       "supply_ecosystem_intelligence.json",
    "workforce_trends":       "workforce_trends_intelligence.json",
    "white_papers":           "industry_white_papers.json",
    "joveo_2026_benchmarks":  "joveo_2026_benchmarks.json",
    "google_ads_benchmarks":  "google_ads_2025_benchmarks.json",
    "external_benchmarks":    "external_benchmarks_2025.json",
    "client_media_plans":     "client_media_plans_kb.json",
}


def load_knowledge_base() -> dict:
    """Load and merge all knowledge base files into unified dict.

    Thread-safe, cached after first load. Returns merged dict with section
    keys + backward-compat top-level keys, or a minimal dict on failure.
    """
    global _knowledge_base
    with _kb_lock:
        if _knowledge_base is not None:
            return _knowledge_base

        kb = {}
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        loaded_count = 0
        for section_key, filename in KB_FILES.items():
            fpath = os.path.join(data_dir, filename)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    kb[section_key] = json.load(f)
                    loaded_count += 1
                    logger.info("KB loaded %s (%s)", section_key, filename)
            except FileNotFoundError:
                kb[section_key] = {}
                logger.warning("KB file not found: %s", filename)
            except json.JSONDecodeError as e:
                kb[section_key] = {}
                logger.error("KB JSON error in %s: %s", filename, e)
            except Exception as e:
                kb[section_key] = {}
                logger.error("KB load error for %s: %s", filename, e)

        # Backward compatibility: merge core keys to top level
        core = kb.get("core", {})
        for k, v in core.items():
            if k not in kb:
                kb[k] = v

        # Data freshness validation
        stale_sections = []
        try:
            today = datetime.datetime.now()
            max_age_days = 90
            for section_key, section_data in kb.items():
                if not isinstance(section_data, dict):
                    continue
                last_updated_str = None
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
                            stale_sections.append(
                                (section_key, last_updated_str, age_days)
                            )
                    except (ValueError, TypeError):
                        pass
            if stale_sections:
                for skey, sdate, sage in stale_sections:
                    logger.warning(
                        "KB DATA FRESHNESS WARNING: '%s' last updated %s "
                        "(%d days ago, threshold=%d days)",
                        skey, sdate, sage, max_age_days,
                    )
                kb["_freshness_warnings"] = [
                    {"section": s, "last_updated": d, "age_days": a}
                    for s, d, a in stale_sections
                ]
        except Exception as e:
            logger.warning("KB freshness check failed (non-fatal): %s", e)

        logger.info("Knowledge base loaded: %d/%d files, %d total keys",
                    loaded_count, len(KB_FILES), len(kb))
        _knowledge_base = kb
        return kb
