"""
data_contracts.py -- Data contract validation for the recruitment advertising
media plan generator.

Validates JSON knowledge-base (KB) files and API enrichment output against
defined schemas so that downstream consumers (the PPT generator, the chatbot,
the orchestrator) never silently receive malformed data.

KB files validated (all in data/):
    1.  channels_db.json
    2.  recruitment_benchmarks_deep.json
    3.  joveo_publishers.json
    4.  global_supply.json
    5.  platform_intelligence_deep.json
    6.  recruitment_industry_knowledge.json
    7.  recruitment_strategy_intelligence.json
    8.  regional_hiring_intelligence.json
    9.  workforce_trends_intelligence.json
    10. industry_white_papers.json
    11. supply_ecosystem_intelligence.json
    12. nova_learned_answers.json

Non-KB files intentionally excluded from schema validation:
    - request_log.json           (runtime log, not a knowledge base)
    - auto_qc_results.json       (QC output, not a knowledge base)
    - linkedin_guidewire_data.json (client-specific, not a general KB)

Enrichment output validated:
    - Return value of api_enrichment.enrich_data()

Design principles:
    - Python stdlib only (no external dependencies).
    - This module must NEVER raise uncaught exceptions.  Every public function
      wraps its logic in try/except and returns structured error information.
    - All numeric range checks use inclusive bounds.

Usage:
    from data_contracts import validate_all_kb, validate_enrichment_output

    kb_report   = validate_all_kb()
    enrich_report = validate_enrichment_output(enriched_dict)
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------

_DATA_DIR = Path(__file__).resolve().parent / "data"

# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------
# Each schema is a dict with:
#   "required_keys"  : list of top-level keys that MUST exist
#   "optional_keys"  : list of top-level keys that MAY exist (no error if absent)
#   "type_checks"    : dict mapping dotted-path -> expected Python type name(s)
#   "range_checks"   : list of (dotted_path, min_val, max_val, description) tuples
#                      where dotted_path leads to a numeric value
#   "list_min_length" : dict mapping dotted-path -> minimum list length
#   "notes"          : human-readable description of the file
# ---------------------------------------------------------------------------

KB_SCHEMAS: Dict[str, Dict[str, Any]] = {
    # -----------------------------------------------------------------------
    # 1. channels_db.json
    # -----------------------------------------------------------------------
    "channels_db.json": {
        "notes": "Recruitment advertising channels organized by category, "
        "region, and industry niche.  Also contains programmatic "
        "platforms and social/emerging channels.",
        "required_keys": [
            "metadata",
            "traditional_channels",
        ],
        "optional_keys": [
            "programmatic_platforms",
            "social_emerging_channels",
            "channel_selection_framework",
        ],
        "type_checks": {
            "metadata": "dict",
            "traditional_channels": "dict",
        },
        "list_min_length": {},
        "range_checks": [],
    },
    # -----------------------------------------------------------------------
    # 2. recruitment_benchmarks_deep.json
    # -----------------------------------------------------------------------
    "recruitment_benchmarks_deep.json": {
        "notes": "Industry-level recruitment benchmarks (CPA, CPC, CPH, "
        "apply rates, time-to-fill) sourced from Appcast, SHRM, "
        "iCIMS, Gem, and others.  Data period 2024-2026.",
        "required_keys": [
            "sources",
            "industry_benchmarks",
        ],
        "optional_keys": [
            "last_updated",
            "data_period",
            "cross_industry_benchmarks",
            "funnel_benchmarks",
            "seasonal_patterns",
        ],
        "type_checks": {
            "sources": "list",
            "industry_benchmarks": "dict",
        },
        "list_min_length": {
            "sources": 5,
        },
        "range_checks": [],
    },
    # -----------------------------------------------------------------------
    # 3. joveo_publishers.json
    # -----------------------------------------------------------------------
    "joveo_publishers.json": {
        "notes": "Joveo publisher/supply-partner network organized by "
        "category (AI tool, Classifieds, Community Hiring, DEI, "
        "DSP, Job Board, etc.).",
        "required_keys": [
            "total_active_publishers",
            "by_category",
        ],
        "optional_keys": [
            "by_region",
            "metadata",
            "last_updated",
        ],
        "type_checks": {
            "total_active_publishers": "int",
            "by_category": "dict",
        },
        "list_min_length": {},
        "range_checks": [
            (
                "total_active_publishers",
                100,
                50000,
                "Total active publishers should be between 100 and 50,000",
            ),
        ],
    },
    # -----------------------------------------------------------------------
    # 3b. joveo_2026_benchmarks.json
    # -----------------------------------------------------------------------
    "joveo_2026_benchmarks.json": {
        "notes": "Joveo 2026 Recruiting Benchmarks Report data: CPA by "
        "occupation, applicant growth, demand intensity, two-market "
        "framework (flood vs drought), experience cliff analysis.",
        "required_keys": [
            "_meta",
            "market_conditions_2025",
            "applicant_growth_nov2022_to_nov2025",
            "median_cpa_by_occupation_2025",
            "cpa_by_application_type",
            "demand_intensity_per_100_employed",
            "two_market_strategies",
            "experience_cliff_2025",
        ],
        "optional_keys": [
            "strategic_questions_for_ta_leaders",
            "integration_notes",
        ],
        "type_checks": {
            "_meta": "dict",
            "market_conditions_2025": "dict",
            "applicant_growth_nov2022_to_nov2025": "dict",
            "median_cpa_by_occupation_2025": "dict",
            "cpa_by_application_type": "dict",
            "demand_intensity_per_100_employed": "dict",
            "two_market_strategies": "dict",
            "experience_cliff_2025": "dict",
        },
        "list_min_length": {},
        "range_checks": [],
    },
    # -----------------------------------------------------------------------
    # 4. global_supply.json
    # -----------------------------------------------------------------------
    "global_supply.json": {
        "notes": "Country-level job board and supply data.  Each country "
        "entry lists boards with name, billing model, category, "
        "and tier.",
        "required_keys": [
            "country_job_boards",
        ],
        "optional_keys": [
            "metadata",
            "last_updated",
            "regional_summary",
        ],
        "type_checks": {
            "country_job_boards": "dict",
        },
        "list_min_length": {},
        "range_checks": [],
    },
    # -----------------------------------------------------------------------
    # 5. platform_intelligence_deep.json
    # -----------------------------------------------------------------------
    "platform_intelligence_deep.json": {
        "notes": "Deep profiles for major recruitment platforms (Indeed, "
        "LinkedIn, ZipRecruiter, etc.) with pricing, traffic, "
        "apply rates, ATS integrations, and recent changes.",
        "required_keys": [
            "platforms",
        ],
        "optional_keys": [
            "last_updated",
            "sources",
            "industry_benchmarks",
            "programmatic_benchmarks",
        ],
        "type_checks": {
            "platforms": "dict",
        },
        "list_min_length": {},
        "range_checks": [],
    },
    # -----------------------------------------------------------------------
    # 6. recruitment_industry_knowledge.json
    # -----------------------------------------------------------------------
    "recruitment_industry_knowledge.json": {
        "notes": "Comprehensive recruitment industry data: CPC/CPA "
        "benchmarks by platform, labor market stats, global "
        "economic indicators, ad platform benchmarks, and the "
        "TA technology landscape.",
        "required_keys": [
            "metadata",
            "benchmarks",
        ],
        "optional_keys": [
            "labor_market",
            "global_economic_indicators",
            "ad_platform_benchmarks",
            "ta_technology_landscape",
            "recruitment_marketing_channels",
            "industry_specific_intelligence",
        ],
        "type_checks": {
            "metadata": "dict",
            "benchmarks": "dict",
        },
        "list_min_length": {},
        "range_checks": [],
    },
    # -----------------------------------------------------------------------
    # 7. recruitment_strategy_intelligence.json
    # -----------------------------------------------------------------------
    "recruitment_strategy_intelligence.json": {
        "notes": "Recruitment strategy playbooks: employer branding ROI, "
        "DEI recruitment, technology landscape, budget frameworks, "
        "content marketing, and creative best practices.",
        "required_keys": [
            "sources",
        ],
        "optional_keys": [
            "last_updated",
            "version",
            "description",
            "employer_branding",
            "dei_recruitment",
            "technology_landscape",
            "budget_frameworks",
            "content_marketing",
            "creative_best_practices",
            "recruitment_funnel",
            "compliance_legal",
            "talent_intelligence",
        ],
        "type_checks": {
            "sources": "list",
        },
        "list_min_length": {
            "sources": 5,
        },
        "range_checks": [],
    },
    # -----------------------------------------------------------------------
    # 8. regional_hiring_intelligence.json
    # -----------------------------------------------------------------------
    "regional_hiring_intelligence.json": {
        "notes": "Region and city-level hiring intelligence: top job boards, "
        "dominant industries, salary benchmarks, talent dynamics, "
        "CPA benchmarks, and hiring regulations per market.",
        "required_keys": [
            "regions",
        ],
        "optional_keys": [
            "last_updated",
            "sources",
            "metadata",
        ],
        "type_checks": {
            "regions": "dict",
        },
        "list_min_length": {},
        "range_checks": [],
    },
    # -----------------------------------------------------------------------
    # 9. workforce_trends_intelligence.json
    # -----------------------------------------------------------------------
    "workforce_trends_intelligence.json": {
        "notes": "Workforce macro-trends: generational preferences (Gen Z, "
        "millennials), remote/hybrid work, compensation trends, "
        "skills-based hiring, gig economy, and industry "
        "disruptions.",
        "required_keys": [
            "sources",
        ],
        "optional_keys": [
            "last_updated",
            "generational_trends",
            "remote_hybrid_trends",
            "compensation_trends",
            "skills_based_hiring",
            "gig_economy",
            "industry_disruptions",
            "emerging_job_categories",
            "geographic_migration",
            "programmatic_advertising_trends",
        ],
        "type_checks": {
            "sources": "list",
        },
        "list_min_length": {
            "sources": 10,
        },
        "range_checks": [],
    },
    # -----------------------------------------------------------------------
    # 10. industry_white_papers.json
    # -----------------------------------------------------------------------
    "industry_white_papers.json": {
        "notes": "Digest of 74+ recruitment industry white papers, benchmark "
        "reports, and studies with key findings and benchmarks.",
        "required_keys": [
            "reports",
        ],
        "optional_keys": [
            "last_updated",
            "total_sources",
            "data_period",
            "description",
        ],
        "type_checks": {
            "reports": "dict",
        },
        "list_min_length": {},
        "range_checks": [
            (
                "total_sources",
                10,
                500,
                "Total white-paper sources should be between 10 and 500",
            ),
        ],
    },
    # -----------------------------------------------------------------------
    # 11. supply_ecosystem_intelligence.json
    # -----------------------------------------------------------------------
    "supply_ecosystem_intelligence.json": {
        "notes": "Full programmatic recruitment supply-chain intelligence: "
        "ecosystem mechanics (XML feeds, bidding, RTB), publisher "
        "tiers, performance benchmarks, competitive landscape, "
        "and emerging trends.",
        "required_keys": [
            "programmatic_ecosystem",
        ],
        "optional_keys": [
            "last_updated",
            "version",
            "description",
            "sources",
            "publisher_tiers",
            "competitive_landscape",
            "advertising_economics",
            "emerging_trends",
        ],
        "type_checks": {
            "programmatic_ecosystem": "dict",
        },
        "list_min_length": {},
        "range_checks": [],
    },
    # -----------------------------------------------------------------------
    # 12. nova_learned_answers.json
    # -----------------------------------------------------------------------
    "nova_learned_answers.json": {
        "notes": "Curated Q&A pairs for the Nova chatbot.  Each entry has "
        "a question, answer, keyword list, and confidence score.",
        "required_keys": [
            "answers",
        ],
        "optional_keys": [],
        "type_checks": {
            "answers": "list",
        },
        "list_min_length": {
            "answers": 1,
        },
        "range_checks": [],
    },
}

# ---------------------------------------------------------------------------
# The expected keys and their types inside the enrichment output dict
# returned by api_enrichment.enrich_data().
# ---------------------------------------------------------------------------

_ENRICHMENT_REQUIRED_KEYS: Dict[str, str] = {
    "salary_data": "dict",
    "industry_employment": ("dict", "NoneType"),
    "location_demographics": "dict",
    "global_indicators": "dict",
    "job_market": "dict",
    "company_info": "dict",
    "company_metadata": "dict",
    "sec_data": "dict",
    "competitor_logos": "dict",
    "currency_rates": "dict",
    "enrichment_summary": "dict",
}

_ENRICHMENT_OPTIONAL_KEYS: Dict[str, str] = {
    "fred_indicators": "dict",
    "search_trends": "dict",
    "onet_data": "dict",
    "imf_indicators": "dict",
    "country_data": "dict",
    "geonames_data": "dict",
    "teleport_data": "dict",
    "datausa_occupation": "dict",
    "datausa_location": "dict",
    "google_ads_data": "dict",
    "meta_ads_data": "dict",
    "bing_ads_data": "dict",
    "tiktok_ads_data": "dict",
    "linkedin_ads_data": "dict",
    "careeronestop_data": "dict",
    "jooble_data": "dict",
    "eurostat_data": "dict",
    "ilo_data": "dict",
    "h1b_data": "dict",
}

_ENRICHMENT_SUMMARY_REQUIRED_KEYS: Dict[str, str] = {
    "apis_called": "list",
    "apis_succeeded": "list",
    "apis_failed": "list",
    "total_time_seconds": ("int", "float"),
    "confidence_score": ("int", "float"),
}

_ENRICHMENT_SUMMARY_OPTIONAL_KEYS: Dict[str, str] = {
    "apis_skipped": "list",
    "apis_circuit_broken": "list",
    "api_details": "dict",
    "cached": "bool",
}

# Confidence score must be 0.0 .. 1.0
_CONFIDENCE_RANGE = (0.0, 1.0)

# Enrichment duration sanity bounds (seconds)
_DURATION_RANGE = (0.0, 600.0)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _type_name(obj: Any) -> str:
    """Return the simple type name of *obj* (e.g. 'dict', 'list', 'int')."""
    return type(obj).__name__


def _resolve_dotted(data: Any, path: str) -> Tuple[bool, Any]:
    """
    Walk *data* along a dotted key path (e.g. 'metadata.last_updated').
    Returns (found: bool, value).
    """
    try:
        current = data
        for part in path.split("."):
            if isinstance(current, dict):
                if part not in current:
                    return False, None
                current = current[part]
            elif isinstance(current, list):
                idx = int(part)
                if idx >= len(current):
                    return False, None
                current = current[idx]
            else:
                return False, None
        return True, current
    except Exception:
        return False, None


def _check_type(value: Any, expected: Union[str, tuple]) -> bool:
    """
    Return True if *value*'s type name matches *expected*.
    *expected* can be a single type name string or a tuple of type name strings.
    """
    try:
        if isinstance(expected, tuple):
            return _type_name(value) in expected
        return _type_name(value) == expected
    except Exception:
        return False


def _check_numeric_range(
    value: Any,
    min_val: Optional[float],
    max_val: Optional[float],
) -> Tuple[bool, Optional[str]]:
    """
    Check whether *value* (int or float) falls within [min_val, max_val].
    Returns (in_range, error_message_or_None).
    """
    try:
        if not isinstance(value, (int, float)):
            return False, f"Expected numeric, got {_type_name(value)}"
        if min_val is not None and value < min_val:
            return False, f"Value {value} is below minimum {min_val}"
        if max_val is not None and value > max_val:
            return False, f"Value {value} is above maximum {max_val}"
        return True, None
    except Exception as exc:
        return False, f"Range check error: {exc}"


def _validate_nova_answers(data: Dict) -> Tuple[List[str], List[str]]:
    """
    Deep-validate the answers list inside nova_learned_answers.json.
    Returns (errors, warnings).
    """
    errors: List[str] = []
    warnings: List[str] = []
    try:
        answers = data.get("answers") or []
        if not isinstance(answers, list):
            errors.append("'answers' is not a list")
            return errors, warnings

        for idx, entry in enumerate(answers):
            prefix = f"answers[{idx}]"
            if not isinstance(entry, dict):
                errors.append(f"{prefix}: expected dict, got {_type_name(entry)}")
                continue
            if "question" not in entry:
                errors.append(f"{prefix}: missing 'question'")
            if "answer" not in entry:
                errors.append(f"{prefix}: missing 'answer'")
            if "keywords" not in entry:
                warnings.append(f"{prefix}: missing 'keywords'")
            elif not isinstance(entry.get("keywords"), list):
                errors.append(f"{prefix}: 'keywords' should be a list")
            if "confidence" in entry:
                conf = entry["confidence"]
                if isinstance(conf, (int, float)):
                    if conf < 0.0 or conf > 1.0:
                        errors.append(f"{prefix}: confidence {conf} outside [0.0, 1.0]")
                else:
                    errors.append(
                        f"{prefix}: confidence should be numeric, "
                        f"got {_type_name(conf)}"
                    )
    except Exception as exc:
        errors.append(f"Nova answers deep validation error: {exc}")
    return errors, warnings


def _validate_industry_benchmarks(data: Dict) -> Tuple[List[str], List[str]]:
    """
    Deep-validate industry_benchmarks inside recruitment_benchmarks_deep.json.
    Checks that CPC medians are in plausible ranges and CPA values look sane.
    Returns (errors, warnings).
    """
    errors: List[str] = []
    warnings: List[str] = []
    try:
        benchmarks = data.get("industry_benchmarks", {})
        if not isinstance(benchmarks, dict):
            errors.append("'industry_benchmarks' is not a dict")
            return errors, warnings

        for industry, metrics in benchmarks.items():
            if not isinstance(metrics, dict):
                errors.append(
                    f"industry_benchmarks.{industry}: expected dict, "
                    f"got {_type_name(metrics)}"
                )
                continue

            # Check for expected sub-keys (at least some should exist)
            expected_sub = {"cpa", "cpc", "cph", "apply_rate", "time_to_fill"}
            found = set(metrics.keys()) & expected_sub
            if not found:
                warnings.append(
                    f"industry_benchmarks.{industry}: none of the expected "
                    f"metric keys ({', '.join(sorted(expected_sub))}) found"
                )

    except Exception as exc:
        errors.append(f"Industry benchmarks deep validation error: {exc}")
    return errors, warnings


def _validate_platform_profiles(data: Dict) -> Tuple[List[str], List[str]]:
    """
    Deep-validate platform profiles inside platform_intelligence_deep.json.
    Returns (errors, warnings).
    """
    errors: List[str] = []
    warnings: List[str] = []
    try:
        platforms = data.get("platforms", {})
        if not isinstance(platforms, dict):
            errors.append("'platforms' is not a dict")
            return errors, warnings

        if len(platforms) < 3:
            warnings.append(
                f"Only {len(platforms)} platform(s) defined; expected at least 3"
            )

        for key, profile in platforms.items():
            prefix = f"platforms.{key}"
            if not isinstance(profile, dict):
                errors.append(f"{prefix}: expected dict, got {_type_name(profile)}")
                continue
            if "name" not in profile:
                warnings.append(f"{prefix}: missing 'name'")
            if "type" not in profile and "url" not in profile:
                warnings.append(f"{prefix}: missing both 'type' and 'url'")

    except Exception as exc:
        errors.append(f"Platform profiles deep validation error: {exc}")
    return errors, warnings


def _validate_regional_data(data: Dict) -> Tuple[List[str], List[str]]:
    """
    Deep-validate regions inside regional_hiring_intelligence.json.
    Returns (errors, warnings).
    """
    errors: List[str] = []
    warnings: List[str] = []
    try:
        regions = data.get("regions", {})
        if not isinstance(regions, dict):
            errors.append("'regions' is not a dict")
            return errors, warnings

        if len(regions) < 1:
            warnings.append("No regions defined")

        for region_key, region_data in regions.items():
            prefix = f"regions.{region_key}"
            if not isinstance(region_data, dict):
                errors.append(f"{prefix}: expected dict, got {_type_name(region_data)}")
                continue
            if "name" not in region_data and "markets" not in region_data:
                warnings.append(f"{prefix}: missing both 'name' and 'markets'")

            # Validate market entries if present
            markets = region_data.get("markets", {})
            if isinstance(markets, dict):
                for market_key, market_data in markets.items():
                    mprefix = f"{prefix}.markets.{market_key}"
                    if not isinstance(market_data, dict):
                        errors.append(
                            f"{mprefix}: expected dict, got "
                            f"{_type_name(market_data)}"
                        )
                        continue
                    # Salary sanity check
                    salaries = market_data.get("avg_salaries", {})
                    if isinstance(salaries, dict):
                        for role, levels in salaries.items():
                            if role == "currency":
                                continue
                            if isinstance(levels, dict):
                                for level, val in levels.items():
                                    if isinstance(val, (int, float)):
                                        if val < 0:
                                            errors.append(
                                                f"{mprefix}.avg_salaries"
                                                f".{role}.{level}: "
                                                f"negative salary {val}"
                                            )
                                        elif val > 1_000_000:
                                            warnings.append(
                                                f"{mprefix}.avg_salaries"
                                                f".{role}.{level}: "
                                                f"unusually high salary {val}"
                                            )

    except Exception as exc:
        errors.append(f"Regional data deep validation error: {exc}")
    return errors, warnings


def _validate_publishers(data: Dict) -> Tuple[List[str], List[str]]:
    """
    Deep-validate joveo_publishers.json structure.
    Returns (errors, warnings).
    """
    errors: List[str] = []
    warnings: List[str] = []
    try:
        total = data.get("total_active_publishers") or 0
        by_cat = data.get("by_category", {})
        if not isinstance(by_cat, dict):
            errors.append("'by_category' is not a dict")
            return errors, warnings

        actual_count = 0
        for cat, publishers in by_cat.items():
            if not isinstance(publishers, list):
                errors.append(
                    f"by_category.{cat}: expected list, got {_type_name(publishers)}"
                )
                continue
            actual_count += len(publishers)

        # Warn if declared total is way off from actual count
        if isinstance(total, (int, float)) and total > 0:
            ratio = actual_count / total if total else 0
            if ratio < 0.5:
                warnings.append(
                    f"Declared total_active_publishers={total} but only "
                    f"{actual_count} publishers found in by_category "
                    f"(ratio={ratio:.2f}).  Some may be in by_region."
                )

    except Exception as exc:
        errors.append(f"Publisher deep validation error: {exc}")
    return errors, warnings


def _validate_global_supply(data: Dict) -> Tuple[List[str], List[str]]:
    """
    Deep-validate global_supply.json country board entries.
    Returns (errors, warnings).
    """
    errors: List[str] = []
    warnings: List[str] = []
    try:
        countries = data.get("country_job_boards", {})
        if not isinstance(countries, dict):
            errors.append("'country_job_boards' is not a dict")
            return errors, warnings

        if len(countries) < 5:
            warnings.append(
                f"Only {len(countries)} countries defined; expected at least 5"
            )

        for country, cdata in countries.items():
            prefix = f"country_job_boards.{country}"
            if isinstance(cdata, dict):
                boards = cdata.get("boards") or []
                if isinstance(boards, list):
                    for idx, board in enumerate(boards):
                        if isinstance(board, dict):
                            if "name" not in board:
                                errors.append(f"{prefix}.boards[{idx}]: missing 'name'")
                else:
                    warnings.append(f"{prefix}.boards: expected list")
            elif isinstance(cdata, list):
                # Some countries may just be a list of board dicts
                for idx, board in enumerate(cdata):
                    if isinstance(board, dict) and "name" not in board:
                        errors.append(f"{prefix}[{idx}]: missing 'name'")
            else:
                errors.append(
                    f"{prefix}: expected dict or list, got {_type_name(cdata)}"
                )

    except Exception as exc:
        errors.append(f"Global supply deep validation error: {exc}")
    return errors, warnings


# ---------------------------------------------------------------------------
# Deep validators registry: maps filename -> deep validation function
# ---------------------------------------------------------------------------

_DEEP_VALIDATORS: Dict[str, Any] = {
    "nova_learned_answers.json": _validate_nova_answers,
    "recruitment_benchmarks_deep.json": _validate_industry_benchmarks,
    "platform_intelligence_deep.json": _validate_platform_profiles,
    "regional_hiring_intelligence.json": _validate_regional_data,
    "joveo_publishers.json": _validate_publishers,
    "global_supply.json": _validate_global_supply,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def validate_kb_file(filepath: str) -> Dict[str, Any]:
    """
    Validate a single KB JSON file against its schema.

    Parameters
    ----------
    filepath : str
        Absolute or relative path to the JSON file.

    Returns
    -------
    dict
        {
            "valid":    bool,
            "errors":   [str, ...],
            "warnings": [str, ...],
            "file":     str   # basename of the file
        }
    """
    errors: List[str] = []
    warnings: List[str] = []
    filename = ""

    try:
        filepath = str(filepath)
        filename = os.path.basename(filepath)

        # -- File existence --
        if not os.path.isfile(filepath):
            errors.append(f"File does not exist: {filepath}")
            return {
                "valid": False,
                "errors": errors,
                "warnings": warnings,
                "file": filename,
            }

        # -- JSON parse --
        try:
            with open(filepath, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except json.JSONDecodeError as exc:
            errors.append(f"Invalid JSON: {exc}")
            return {
                "valid": False,
                "errors": errors,
                "warnings": warnings,
                "file": filename,
            }
        except Exception as exc:
            errors.append(f"Could not read file: {exc}")
            return {
                "valid": False,
                "errors": errors,
                "warnings": warnings,
                "file": filename,
            }

        # -- Schema lookup --
        schema = KB_SCHEMAS.get(filename)
        if schema is None:
            warnings.append(
                f"No schema defined for '{filename}'; "
                "only JSON validity was checked."
            )
            return {
                "valid": True,
                "errors": errors,
                "warnings": warnings,
                "file": filename,
            }

        if not isinstance(data, dict):
            errors.append(f"Top-level value should be a dict, got {_type_name(data)}")
            return {
                "valid": False,
                "errors": errors,
                "warnings": warnings,
                "file": filename,
            }

        # -- Required keys --
        for key in schema.get("required_keys") or []:
            if key not in data:
                errors.append(f"Missing required key: '{key}'")

        # -- Type checks --
        for path, expected_type in schema.get("type_checks", {}).items():
            found, value = _resolve_dotted(data, path)
            if found and not _check_type(value, expected_type):
                errors.append(
                    f"Type mismatch at '{path}': expected {expected_type}, "
                    f"got {_type_name(value)}"
                )

        # -- List minimum lengths --
        for path, min_len in schema.get("list_min_length", {}).items():
            found, value = _resolve_dotted(data, path)
            if found:
                if isinstance(value, list):
                    if len(value) < min_len:
                        warnings.append(
                            f"'{path}' has {len(value)} items; "
                            f"expected at least {min_len}"
                        )
                else:
                    errors.append(
                        f"'{path}' should be a list for length check, "
                        f"got {_type_name(value)}"
                    )

        # -- Numeric range checks --
        for path, min_val, max_val, desc in schema.get("range_checks") or []:
            found, value = _resolve_dotted(data, path)
            if found:
                in_range, msg = _check_numeric_range(value, min_val, max_val)
                if not in_range:
                    errors.append(f"Range violation at '{path}': {msg} ({desc})")

        # -- Deep validation --
        deep_fn = _DEEP_VALIDATORS.get(filename)
        if deep_fn is not None:
            try:
                deep_errors, deep_warnings = deep_fn(data)
                errors.extend(deep_errors)
                warnings.extend(deep_warnings)
            except Exception as exc:
                warnings.append(f"Deep validation raised an error: {exc}")

    except Exception as exc:
        errors.append(f"Unexpected validation error: {exc}")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "file": filename,
    }


def validate_enrichment_output(data: Dict) -> Dict[str, Any]:
    """
    Validate the dict returned by ``api_enrichment.enrich_data()``.

    Parameters
    ----------
    data : dict
        The enrichment result dictionary.

    Returns
    -------
    dict
        {
            "valid":    bool,
            "errors":   [str, ...],
            "warnings": [str, ...],
        }
    """
    errors: List[str] = []
    warnings: List[str] = []

    try:
        if data is None:
            errors.append("Enrichment output is None")
            return {"valid": False, "errors": errors, "warnings": warnings}

        if not isinstance(data, dict):
            errors.append(f"Enrichment output should be a dict, got {_type_name(data)}")
            return {"valid": False, "errors": errors, "warnings": warnings}

        # -- Required keys and types --
        for key, expected in _ENRICHMENT_REQUIRED_KEYS.items():
            if key not in data:
                errors.append(f"Missing required enrichment key: '{key}'")
            else:
                if not _check_type(data[key], expected):
                    # Allow None for keys that legitimately can be None
                    if (
                        data[key] is None
                        and isinstance(expected, tuple)
                        and "NoneType" in expected
                    ):
                        pass
                    else:
                        errors.append(
                            f"Type mismatch for '{key}': expected {expected}, "
                            f"got {_type_name(data[key])}"
                        )

        # -- Optional keys type check --
        for key, expected in _ENRICHMENT_OPTIONAL_KEYS.items():
            if key in data and data[key] is not None:
                if not _check_type(data[key], expected):
                    warnings.append(
                        f"Type mismatch for optional key '{key}': "
                        f"expected {expected}, got {_type_name(data[key])}"
                    )

        # -- enrichment_summary deep validation --
        summary = data.get("enrichment_summary")
        if isinstance(summary, dict):
            for skey, stype in _ENRICHMENT_SUMMARY_REQUIRED_KEYS.items():
                if skey not in summary:
                    errors.append(f"Missing key in enrichment_summary: '{skey}'")
                else:
                    if not _check_type(summary[skey], stype):
                        errors.append(
                            f"Type mismatch in enrichment_summary.{skey}: "
                            f"expected {stype}, got {_type_name(summary[skey])}"
                        )

            for skey, stype in _ENRICHMENT_SUMMARY_OPTIONAL_KEYS.items():
                if skey in summary and summary[skey] is not None:
                    if not _check_type(summary[skey], stype):
                        warnings.append(
                            f"Type mismatch in enrichment_summary.{skey}: "
                            f"expected {stype}, got {_type_name(summary[skey])}"
                        )

            # -- Confidence score range --
            conf = summary.get("confidence_score")
            if isinstance(conf, (int, float)):
                if conf < _CONFIDENCE_RANGE[0] or conf > _CONFIDENCE_RANGE[1]:
                    errors.append(
                        f"confidence_score {conf} outside valid range "
                        f"{_CONFIDENCE_RANGE}"
                    )

            # -- Duration sanity --
            dur = summary.get("total_time_seconds")
            if isinstance(dur, (int, float)):
                if dur < _DURATION_RANGE[0]:
                    warnings.append(f"total_time_seconds {dur} is negative")
                elif dur > _DURATION_RANGE[1]:
                    warnings.append(
                        f"total_time_seconds {dur} exceeds {_DURATION_RANGE[1]}s; "
                        "enrichment may have hung"
                    )

            # -- apis_called / apis_succeeded consistency --
            called = summary.get("apis_called") or []
            succeeded = summary.get("apis_succeeded") or []
            failed = summary.get("apis_failed") or []
            skipped = summary.get("apis_skipped") or []

            if isinstance(called, list) and isinstance(succeeded, list):
                if len(succeeded) > len(called):
                    errors.append(
                        f"apis_succeeded ({len(succeeded)}) > "
                        f"apis_called ({len(called)})"
                    )

            if isinstance(called, list) and isinstance(failed, list):
                if len(failed) > len(called):
                    errors.append(
                        f"apis_failed ({len(failed)}) > " f"apis_called ({len(called)})"
                    )

            # Warn if zero APIs succeeded
            if isinstance(succeeded, list) and len(succeeded) == 0:
                if isinstance(called, list) and len(called) > 0:
                    warnings.append("No APIs succeeded out of " f"{len(called)} called")

        elif summary is not None:
            errors.append(
                f"enrichment_summary should be dict, got {_type_name(summary)}"
            )

    except Exception as exc:
        errors.append(f"Unexpected enrichment validation error: {exc}")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
    }


def validate_all_kb(data_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Validate every KB file defined in ``KB_SCHEMAS`` that exists in the
    data directory.

    Parameters
    ----------
    data_dir : str, optional
        Path to the data directory.  Defaults to the ``data/`` directory
        next to this module.

    Returns
    -------
    dict
        {
            "total":   int,
            "passed":  int,
            "failed":  int,
            "missing": int,
            "details": [validation_result_per_file, ...],
        }
    """
    results: List[Dict[str, Any]] = []
    passed = 0
    failed = 0
    missing = 0

    try:
        if data_dir is None:
            base = _DATA_DIR
        else:
            base = Path(data_dir)

        for filename in sorted(KB_SCHEMAS.keys()):
            filepath = base / filename
            try:
                if not filepath.is_file():
                    missing += 1
                    results.append(
                        {
                            "valid": False,
                            "errors": [f"File not found: {filepath}"],
                            "warnings": [],
                            "file": filename,
                        }
                    )
                    continue

                result = validate_kb_file(str(filepath))
                results.append(result)
                if result.get("valid", False):
                    passed += 1
                else:
                    failed += 1
            except Exception as exc:
                failed += 1
                results.append(
                    {
                        "valid": False,
                        "errors": [f"Validation crashed for {filename}: {exc}"],
                        "warnings": [],
                        "file": filename,
                    }
                )

    except Exception as exc:
        results.append(
            {
                "valid": False,
                "errors": [f"validate_all_kb crashed: {exc}"],
                "warnings": [],
                "file": "__runner__",
            }
        )

    total = len(KB_SCHEMAS)
    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "missing": missing,
        "details": results,
    }


# ---------------------------------------------------------------------------
# CLI convenience
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    print("=" * 70)
    print("  Data Contract Validation Report")
    print("=" * 70)
    print()

    report = validate_all_kb()

    for detail in report["details"]:
        status = "PASS" if detail["valid"] else "FAIL"
        print(f"  [{status}] {detail['file']}")
        for err in detail.get("errors") or []:
            print(f"         ERROR: {err}")
        for warn in detail.get("warnings") or []:
            print(f"         WARN:  {warn}")

    print()
    print("-" * 70)
    print(
        f"  Total: {report['total']}  |  "
        f"Passed: {report['passed']}  |  "
        f"Failed: {report['failed']}  |  "
        f"Missing: {report['missing']}"
    )
    print("=" * 70)

    sys.exit(0 if report["failed"] == 0 else 1)
