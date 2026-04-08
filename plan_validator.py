"""Cross-validation post-processing layer for generated media plans.

Runs after all enrichment, budget allocation, and gold-standard quality gates,
but BEFORE Excel/PPT generation.  Detects internal inconsistencies, flags
findings, and auto-corrects where safe to do so.

The main entry point is ``validate_plan(data)`` which mutates ``data`` in-place
(adding ``data["_validation"]``) and returns a summary dict.

Each check is isolated in its own try/except so a failure in one check never
blocks the others.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Import role salary ranges from gold_standard (fallback to empty dict)
# ---------------------------------------------------------------------------
try:
    from gold_standard import _ROLE_SALARY_RANGES, _COUNTRY_SALARY_MULTIPLIERS
except ImportError:
    _ROLE_SALARY_RANGES: dict[str, tuple[int, int]] = {}
    _COUNTRY_SALARY_MULTIPLIERS: dict[str, float] = {}
    logger.warning(
        "gold_standard import failed -- salary/country checks will be limited"
    )

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Countries that should never map to "United States" in city_level_data.
# Lowercase canonical names matching _COUNTRY_SALARY_MULTIPLIERS keys.
_KNOWN_COUNTRY_NAMES: set[str] = set(_COUNTRY_SALARY_MULTIPLIERS.keys()) | {
    "canada",
    "india",
    "china",
    "japan",
    "germany",
    "united kingdom",
    "uk",
    "australia",
    "singapore",
    "brazil",
    "mexico",
    "philippines",
    "france",
    "netherlands",
    "south korea",
    "taiwan",
    "israel",
    "uae",
    "ireland",
    "spain",
    "italy",
    "sweden",
    "switzerland",
    "norway",
    "new zealand",
    "vietnam",
    "thailand",
    "indonesia",
    "malaysia",
    "colombia",
    "argentina",
    "chile",
    "south africa",
    "nigeria",
    "kenya",
    "egypt",
    "turkey",
    "romania",
    "czech republic",
    "hungary",
    "portugal",
    "belgium",
    "austria",
    "denmark",
    "finland",
    "poland",
    "saudi arabia",
    "united arab emirates",
    "russia",
    "ukraine",
    "pakistan",
    "bangladesh",
    "sri lanka",
    "nepal",
    "cambodia",
    "myanmar",
}

# CPA discrepancy tolerance (15%)
_CPA_TOLERANCE = 0.15

# Hires discrepancy tolerance (5%)
_HIRES_TOLERANCE = 0.05

# Salary outlier multipliers
_SALARY_UPPER_MULT = 2.0
_SALARY_LOWER_MULT = 0.5


# ---------------------------------------------------------------------------
# Individual check functions
# ---------------------------------------------------------------------------


def _check_salary_vs_role(data: dict) -> list[dict[str, Any]]:
    """Check 1: Salary vs Role consistency.

    For each city in ``_gold_standard.city_level_data``, verify that salary
    estimates are plausible for the plan's roles.  If a salary exceeds 2x the
    role ceiling or falls below 0.5x the role floor, flag and auto-correct
    to the role midpoint (city-adjusted).
    """
    findings: list[dict[str, Any]] = []
    gold = data.get("_gold_standard") or {}
    city_data = gold.get("city_level_data") or {}
    if not city_data or not _ROLE_SALARY_RANGES:
        return findings

    roles_raw = data.get("target_roles") or data.get("roles") or []
    role_titles: list[str] = []
    for r in (roles_raw if isinstance(roles_raw, list) else [str(roles_raw)]):
        if isinstance(r, str) and r.strip():
            role_titles.append(r.strip())
        elif isinstance(r, dict):
            t = str(r.get("title") or "").strip()
            if t:
                role_titles.append(t)

    for city_name, city_info in city_data.items():
        if not isinstance(city_info, dict):
            continue
        est_salary = city_info.get("estimated_salary") or 0
        multiplier = city_info.get("salary_multiplier") or 1.0
        if est_salary <= 0:
            continue

        for title in role_titles:
            title_lower = title.lower().strip()
            matched_range: tuple[int, int] | None = None
            for keyword in sorted(_ROLE_SALARY_RANGES, key=len, reverse=True):
                if keyword in title_lower:
                    matched_range = _ROLE_SALARY_RANGES[keyword]
                    break

            if matched_range is None:
                continue

            adjusted_floor = matched_range[0] * multiplier
            adjusted_ceiling = matched_range[1] * multiplier
            midpoint = (matched_range[0] + matched_range[1]) / 2.0 * multiplier

            if est_salary > adjusted_ceiling * _SALARY_UPPER_MULT:
                corrected = round(midpoint)
                findings.append(
                    {
                        "check": "salary_vs_role",
                        "severity": "high",
                        "city": city_name,
                        "role": title,
                        "message": (
                            f"Salary ${est_salary:,.0f} exceeds {_SALARY_UPPER_MULT}x "
                            f"ceiling ${adjusted_ceiling:,.0f} for '{title}' in {city_name}"
                        ),
                        "auto_corrected": True,
                        "old_value": est_salary,
                        "new_value": corrected,
                    }
                )
                city_info["estimated_salary"] = corrected
                city_info["_validator_corrected_salary"] = True
                logger.info(
                    "Validator: Clamped salary %s -> %s for %s in %s",
                    est_salary,
                    corrected,
                    title,
                    city_name,
                )
            elif est_salary < adjusted_floor * _SALARY_LOWER_MULT:
                corrected = round(midpoint)
                findings.append(
                    {
                        "check": "salary_vs_role",
                        "severity": "medium",
                        "city": city_name,
                        "role": title,
                        "message": (
                            f"Salary ${est_salary:,.0f} below {_SALARY_LOWER_MULT}x "
                            f"floor ${adjusted_floor:,.0f} for '{title}' in {city_name}"
                        ),
                        "auto_corrected": True,
                        "old_value": est_salary,
                        "new_value": corrected,
                    }
                )
                city_info["estimated_salary"] = corrected
                city_info["_validator_corrected_salary"] = True
                logger.info(
                    "Validator: Raised salary %s -> %s for %s in %s",
                    est_salary,
                    corrected,
                    title,
                    city_name,
                )

    return findings


def _check_demand_vs_temperature(data: dict) -> list[dict[str, Any]]:
    """Check 2: Demand vs Temperature consistency.

    In ``_synthesized.job_market_demand``, verify that market_temperature
    aligns with total_postings.  Zero postings should not be "hot"; 50K+
    postings should not be "cold".
    """
    findings: list[dict[str, Any]] = []
    synthesized = data.get("_synthesized") or {}
    demand = synthesized.get("job_market_demand") or {}
    if not isinstance(demand, dict):
        return findings

    # job_market_demand may be keyed by role or be a flat dict
    role_dicts: list[tuple[str, dict]] = []
    for key, val in demand.items():
        if isinstance(val, dict) and "total_postings" in val:
            role_dicts.append((key, val))

    # If the top-level demand dict itself has total_postings, check it too
    if "total_postings" in demand:
        role_dicts.append(("_overall", demand))

    for role_key, role_data in role_dicts:
        total_postings = int(role_data.get("total_postings") or 0)
        temperature = str(role_data.get("market_temperature") or "").lower().strip()
        if not temperature:
            continue

        corrected_temp: str | None = None

        if total_postings == 0 and temperature == "hot":
            corrected_temp = "cold"
            findings.append(
                {
                    "check": "demand_vs_temperature",
                    "severity": "high",
                    "role": role_key,
                    "message": (
                        f"Temperature is 'hot' but total_postings=0 for '{role_key}'"
                    ),
                    "auto_corrected": True,
                    "old_value": temperature,
                    "new_value": corrected_temp,
                }
            )
        elif total_postings == 0 and temperature in ("warm", "hot"):
            corrected_temp = "cold"
            findings.append(
                {
                    "check": "demand_vs_temperature",
                    "severity": "medium",
                    "role": role_key,
                    "message": (
                        f"Temperature is '{temperature}' but total_postings=0 "
                        f"for '{role_key}'"
                    ),
                    "auto_corrected": True,
                    "old_value": temperature,
                    "new_value": corrected_temp,
                }
            )
        elif total_postings > 50_000 and temperature == "cold":
            corrected_temp = "hot"
            findings.append(
                {
                    "check": "demand_vs_temperature",
                    "severity": "medium",
                    "role": role_key,
                    "message": (
                        f"Temperature is 'cold' but total_postings={total_postings:,} "
                        f"for '{role_key}'"
                    ),
                    "auto_corrected": True,
                    "old_value": temperature,
                    "new_value": corrected_temp,
                }
            )

        if corrected_temp is not None:
            role_data["market_temperature"] = corrected_temp
            role_data["_validator_corrected_temperature"] = True
            logger.info(
                "Validator: Temperature %s -> %s for %s (postings=%d)",
                temperature,
                corrected_temp,
                role_key,
                total_postings,
            )

    return findings


def _check_cpa_vs_budget(data: dict) -> list[dict[str, Any]]:
    """Check 3: CPA vs Budget math consistency.

    For each channel allocation, verify that dollar_amount / projected_applications
    roughly equals the stated CPA.  Flag discrepancies > 15% and recompute.
    """
    findings: list[dict[str, Any]] = []
    budget_alloc = data.get("_budget_allocation") or {}
    ch_allocs = budget_alloc.get("channel_allocations") or {}

    for ch_name, ch_data in ch_allocs.items():
        if not isinstance(ch_data, dict):
            continue

        dollar_amount = float(ch_data.get("dollar_amount") or 0)
        projected_apps = int(ch_data.get("projected_applications") or 0)
        stated_cpa = float(ch_data.get("cpa") or 0)

        if dollar_amount <= 0 or projected_apps <= 0 or stated_cpa <= 0:
            continue

        computed_cpa = dollar_amount / max(projected_apps, 1)
        discrepancy = abs(computed_cpa - stated_cpa) / stated_cpa

        if discrepancy > _CPA_TOLERANCE:
            findings.append(
                {
                    "check": "cpa_vs_budget",
                    "severity": "high",
                    "channel": ch_name,
                    "message": (
                        f"CPA mismatch for '{ch_name}': stated=${stated_cpa:.2f}, "
                        f"computed=${computed_cpa:.2f} "
                        f"(discrepancy={discrepancy:.1%})"
                    ),
                    "auto_corrected": True,
                    "old_value": round(stated_cpa, 2),
                    "new_value": round(computed_cpa, 2),
                }
            )
            ch_data["cpa"] = round(computed_cpa, 2)
            ch_data["_validator_corrected_cpa"] = True
            logger.info(
                "Validator: CPA corrected %s -> %s for %s",
                stated_cpa,
                round(computed_cpa, 2),
                ch_name,
            )

    return findings


def _check_confidence_consistency(data: dict) -> list[dict[str, Any]]:
    """Check 4: Confidence consistency across channels.

    If overall enrichment confidence < 0.5, no channel should claim "high"
    confidence.  If < 0.3, all channels should be "low".
    """
    findings: list[dict[str, Any]] = []
    enriched = data.get("_enriched") or {}
    enr_summary = (
        enriched.get("enrichment_summary", {}) if isinstance(enriched, dict) else {}
    )
    overall_confidence = float(
        enr_summary.get("confidence_score", 1.0)
        if isinstance(enr_summary, dict)
        else 1.0
    )

    budget_alloc = data.get("_budget_allocation") or {}
    ch_allocs = budget_alloc.get("channel_allocations") or {}

    for ch_name, ch_data in ch_allocs.items():
        if not isinstance(ch_data, dict):
            continue
        ch_confidence = str(ch_data.get("confidence") or "").lower().strip()
        if not ch_confidence:
            continue

        corrected: str | None = None

        if overall_confidence < 0.3 and ch_confidence != "low":
            corrected = "low"
            findings.append(
                {
                    "check": "confidence_consistency",
                    "severity": "medium",
                    "channel": ch_name,
                    "message": (
                        f"Overall confidence={overall_confidence:.2f} (<0.3) "
                        f"but '{ch_name}' has confidence='{ch_confidence}'"
                    ),
                    "auto_corrected": True,
                    "old_value": ch_confidence,
                    "new_value": corrected,
                }
            )
        elif overall_confidence < 0.5 and ch_confidence == "high":
            corrected = "medium"
            findings.append(
                {
                    "check": "confidence_consistency",
                    "severity": "low",
                    "channel": ch_name,
                    "message": (
                        f"Overall confidence={overall_confidence:.2f} (<0.5) "
                        f"but '{ch_name}' has confidence='high'"
                    ),
                    "auto_corrected": True,
                    "old_value": ch_confidence,
                    "new_value": corrected,
                }
            )

        if corrected is not None:
            ch_data["confidence"] = corrected
            ch_data["_validator_corrected_confidence"] = True
            logger.info(
                "Validator: Confidence %s -> %s for %s (overall=%.2f)",
                ch_confidence,
                corrected,
                ch_name,
                overall_confidence,
            )

    return findings


def _check_hires_consistency(data: dict) -> list[dict[str, Any]]:
    """Check 5: Hires consistency across sheets.

    The sum of per-channel projected_hires should roughly equal the total_hires
    in the budget metadata.  Flag discrepancies > 5%.
    """
    findings: list[dict[str, Any]] = []
    budget_alloc = data.get("_budget_allocation") or {}
    ch_allocs = budget_alloc.get("channel_allocations") or {}
    meta = budget_alloc.get("metadata") or {}

    # Try to find total hires from metadata or synthesized data
    summary_total_hires = float(meta.get("total_projected_hires") or 0)
    if summary_total_hires <= 0:
        summary_total_hires = float(meta.get("total_hires") or 0)
    if summary_total_hires <= 0:
        # Check synthesized projection data
        synthesized = data.get("_synthesized") or {}
        summary_total_hires = float(synthesized.get("projected_hires") or 0)

    if summary_total_hires <= 0:
        return findings  # No summary total to compare against

    channel_hires_sum = 0.0
    for ch_name, ch_data in ch_allocs.items():
        if not isinstance(ch_data, dict):
            continue
        ch_hires = float(ch_data.get("projected_hires") or 0)
        channel_hires_sum += ch_hires

    if channel_hires_sum <= 0:
        return findings  # No channel-level hires data

    discrepancy = abs(channel_hires_sum - summary_total_hires) / summary_total_hires

    if discrepancy > _HIRES_TOLERANCE:
        findings.append(
            {
                "check": "hires_consistency",
                "severity": "medium",
                "message": (
                    f"Per-channel hires sum ({channel_hires_sum:.0f}) differs from "
                    f"summary total ({summary_total_hires:.0f}) by {discrepancy:.1%}"
                ),
                "auto_corrected": False,
                "channel_sum": round(channel_hires_sum),
                "summary_total": round(summary_total_hires),
                "discrepancy_pct": round(discrepancy * 100, 1),
            }
        )
        logger.warning(
            "Validator: Hires mismatch -- channels=%d vs summary=%d (%.1f%%)",
            channel_hires_sum,
            summary_total_hires,
            discrepancy * 100,
        )

    return findings


def _check_location_sanity(data: dict) -> list[dict[str, Any]]:
    """Check 6: Location sanity.

    Flags two issues:
      a) A location in city_level_data that matches a known country name
         (e.g. "India") -- these should not be treated as US cities.
      b) Duplicate/templated city entries where ALL fields are identical
         to another city (sign of copy-paste data generation).
    """
    findings: list[dict[str, Any]] = []
    gold = data.get("_gold_standard") or {}
    city_data = gold.get("city_level_data") or {}
    if not city_data:
        return findings

    # Check (a): country names in city-level data
    for city_name in city_data:
        city_key = city_name.lower().strip()
        if city_key in _KNOWN_COUNTRY_NAMES:
            findings.append(
                {
                    "check": "location_sanity",
                    "severity": "low",
                    "city": city_name,
                    "message": (
                        f"'{city_name}' is a known country name in city_level_data -- "
                        f"verify it is not incorrectly treated as a US city"
                    ),
                    "auto_corrected": False,
                }
            )

    # Check (b): duplicate/templated city data
    # Build a fingerprint of each city's numeric + string fields (excluding
    # per_role_salary which is expected to differ).  Two cities with an
    # identical fingerprint indicate templated data.
    fingerprints: dict[str, list[str]] = {}
    for city_name, city_info in city_data.items():
        if not isinstance(city_info, dict):
            continue
        fp_parts = [
            str(city_info.get("salary_multiplier") or ""),
            str(city_info.get("estimated_salary") or ""),
            str(city_info.get("hiring_difficulty") or ""),
            str(city_info.get("supply_tier") or ""),
            str(city_info.get("cost_of_living_index") or ""),
        ]
        fp = "|".join(fp_parts)
        fingerprints.setdefault(fp, []).append(city_name)

    for fp, cities in fingerprints.items():
        if len(cities) > 1:
            findings.append(
                {
                    "check": "location_sanity",
                    "severity": "medium",
                    "cities": cities,
                    "message": (
                        f"Cities {cities} have identical data fingerprints -- "
                        f"possible templated/duplicated data"
                    ),
                    "auto_corrected": False,
                }
            )
            logger.warning(
                "Validator: Duplicate city data detected for %s",
                cities,
            )

    return findings


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def validate_plan(data: dict) -> dict:
    """Run all cross-validation checks on the enriched plan data.

    Mutates ``data`` in-place by adding ``data["_validation"]`` with the
    results.  Each individual check is isolated so a failure in one does
    not prevent the others from running.

    Args:
        data: The full enriched data dict (same dict passed to Excel/PPT
              generators).

    Returns:
        A summary dict with keys:
          - ``findings``: list of all findings across checks
          - ``auto_corrections``: count of auto-corrected issues
          - ``checks_run``: count of checks that executed successfully
          - ``checks_failed``: count of checks that raised exceptions
          - ``severity_counts``: dict of severity -> count
    """
    all_findings: list[dict[str, Any]] = []
    checks_run = 0
    checks_failed = 0

    checks = [
        ("salary_vs_role", _check_salary_vs_role),
        ("demand_vs_temperature", _check_demand_vs_temperature),
        ("cpa_vs_budget", _check_cpa_vs_budget),
        ("confidence_consistency", _check_confidence_consistency),
        ("hires_consistency", _check_hires_consistency),
        ("location_sanity", _check_location_sanity),
    ]

    for check_name, check_fn in checks:
        try:
            results = check_fn(data)
            all_findings.extend(results)
            checks_run += 1
            if results:
                logger.info(
                    "Validator check '%s': %d finding(s)",
                    check_name,
                    len(results),
                )
            else:
                logger.debug("Validator check '%s': clean", check_name)
        except Exception as exc:
            checks_failed += 1
            logger.error(
                "Validator check '%s' failed: %s",
                check_name,
                exc,
                exc_info=True,
            )
            all_findings.append(
                {
                    "check": check_name,
                    "severity": "error",
                    "message": f"Check raised exception: {exc}",
                    "auto_corrected": False,
                }
            )

    auto_corrections = sum(1 for f in all_findings if f.get("auto_corrected"))
    severity_counts: dict[str, int] = {}
    for f in all_findings:
        sev = f.get("severity") or "unknown"
        severity_counts[sev] = severity_counts.get(sev, 0) + 1

    summary = {
        "findings": all_findings,
        "auto_corrections": auto_corrections,
        "checks_run": checks_run,
        "checks_failed": checks_failed,
        "severity_counts": severity_counts,
        "total_findings": len(all_findings),
    }

    data["_validation"] = summary

    if all_findings:
        logger.info(
            "Plan validation complete: %d finding(s), %d auto-corrected, "
            "%d check(s) run, %d failed",
            len(all_findings),
            auto_corrections,
            checks_run,
            checks_failed,
        )
    else:
        logger.info("Plan validation complete: no issues found")

    return summary
