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
import re
from typing import Any

from market_evidence import market_metric_is_observed

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Import role salary ranges from gold_standard (fallback to empty dict)
# ---------------------------------------------------------------------------
try:
    from gold_standard import (
        _ROLE_SALARY_RANGES,
        _COUNTRY_SALARY_MULTIPLIERS,
        _ROLE_SALARY_KEYWORDS_SORTED,
    )
except ImportError:
    _ROLE_SALARY_RANGES: dict[str, tuple[int, int]] = {}
    _COUNTRY_SALARY_MULTIPLIERS: dict[str, float] = {}
    _ROLE_SALARY_KEYWORDS_SORTED: list[str] = []
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

# Channel allocation sum tolerance vs total budget (2%)
_ALLOCATION_TOLERANCE = 0.02

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
            # PERF: Use pre-sorted keyword list from gold_standard instead
            # of sorting on every (city, role) iteration
            matched_range: tuple[int, int] | None = None
            for keyword in _ROLE_SALARY_KEYWORDS_SORTED:
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
    """Check 2: market temperature carries observed evidence.

    Temperature is a postings-to-supply ratio. The validator may remove an
    unsupported label, but must not invent a replacement from posting volume.
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

        is_observed = market_metric_is_observed(role_data, "market_temperature")
        if not is_observed or total_postings <= 0:
            role_data["market_temperature"] = ""
            role_data["_validator_corrected_temperature"] = True
            reason = (
                "no observed market-temperature evidence"
                if not is_observed
                else "no positive observed postings"
            )
            findings.append(
                {
                    "check": "demand_vs_temperature",
                    "severity": "high",
                    "role": role_key,
                    "message": (
                        f"Removed unsupported temperature '{temperature}' for "
                        f"'{role_key}': {reason}"
                    ),
                    "auto_corrected": True,
                    "old_value": temperature,
                    "new_value": "",
                }
            )
            logger.info(
                "Validator: removed temperature %s for %s (postings=%d; %s)",
                temperature,
                role_key,
                total_postings,
                reason,
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


def _check_budget_allocation_sum(data: dict) -> list[dict[str, Any]]:
    """Check 7: Channel allocations must sum to the total budget.

    Rounding drift and post-hoc channel adjustments (rebalancing, CPH floor
    scaling) can leave the sum of per-channel ``dollar_amount`` values above
    or below the campaign's total budget.  Over-allocation is auto-corrected
    by scaling every channel (and its linear projections, so CPA/CPC stay
    internally consistent) down proportionally.  Material under-allocation
    is flagged but left as-is since it may be an intentional contingency
    reserve.
    """
    findings: list[dict[str, Any]] = []
    budget_alloc = data.get("_budget_allocation") or {}
    ch_allocs = budget_alloc.get("channel_allocations") or {}
    meta = budget_alloc.get("metadata") or {}

    total_budget = float(meta.get("total_budget") or 0)
    if total_budget <= 0 or not ch_allocs:
        return findings

    allocated = sum(
        float(ch_data.get("dollar_amount") or 0)
        for ch_data in ch_allocs.values()
        if isinstance(ch_data, dict)
    )
    if allocated <= 0:
        return findings

    drift = (allocated - total_budget) / total_budget

    if drift > _ALLOCATION_TOLERANCE:
        scale = total_budget / allocated
        for ch_data in ch_allocs.values():
            if not isinstance(ch_data, dict):
                continue
            old_amount = float(ch_data.get("dollar_amount") or 0)
            ch_data["dollar_amount"] = round(old_amount * scale, 2)
            # Scale linear projections by the same factor so per-channel
            # CPA/CPC/CPH derived from them remain unchanged.
            for proj_key in (
                "projected_clicks",
                "projected_applications",
                "projected_hires",
            ):
                proj_val = ch_data.get(proj_key)
                if isinstance(proj_val, (int, float)) and proj_val > 0:
                    scaled = proj_val * scale
                    ch_data[proj_key] = (
                        round(scaled, 2)
                        if isinstance(proj_val, float)
                        else max(0, int(round(scaled)))
                    )
            ch_data["_validator_corrected_allocation"] = True

        # Keep aggregate totals consistent with the rescaled channels.
        totals = budget_alloc.get("total_projected")
        if isinstance(totals, dict):
            for tot_key in ("clicks", "applications", "hires"):
                tot_val = totals.get(tot_key)
                if isinstance(tot_val, (int, float)) and tot_val > 0:
                    scaled = tot_val * scale
                    totals[tot_key] = (
                        round(scaled, 2)
                        if isinstance(tot_val, float)
                        else max(0, int(round(scaled)))
                    )
            hires_now = float(totals.get("hires") or 0)
            if hires_now > 0:
                totals["cost_per_hire"] = round(total_budget / hires_now, 2)
            apps_now = float(totals.get("applications") or 0)
            if apps_now > 0:
                totals["cost_per_application"] = round(total_budget / apps_now, 2)
            clicks_now = float(totals.get("clicks") or 0)
            if clicks_now > 0:
                totals["cost_per_click"] = round(total_budget / clicks_now, 2)

        findings.append(
            {
                "check": "budget_allocation_sum",
                "severity": "high",
                "message": (
                    f"Channel allocations (${allocated:,.0f}) exceed total budget "
                    f"(${total_budget:,.0f}) by {drift:.1%} -- rescaled all "
                    f"channels proportionally"
                ),
                "auto_corrected": True,
                "old_value": round(allocated, 2),
                "new_value": round(total_budget, 2),
            }
        )
        logger.info(
            "Validator: Allocations rescaled $%.0f -> $%.0f (drift %.1f%%)",
            allocated,
            total_budget,
            drift * 100,
        )
    elif drift < -_ALLOCATION_TOLERANCE:
        findings.append(
            {
                "check": "budget_allocation_sum",
                "severity": "low",
                "message": (
                    f"Channel allocations (${allocated:,.0f}) leave "
                    f"${total_budget - allocated:,.0f} ({-drift:.1%}) of the "
                    f"total budget (${total_budget:,.0f}) unallocated -- "
                    f"verify this is an intentional contingency reserve"
                ),
                "auto_corrected": False,
                "allocated": round(allocated, 2),
                "total_budget": round(total_budget, 2),
                "unallocated": round(total_budget - allocated, 2),
            }
        )
        logger.warning(
            "Validator: $%.0f (%.1f%%) of budget unallocated across channels",
            total_budget - allocated,
            -drift * 100,
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
            # Root cause of the "Duplicate city data" warning on every real
            # run: same-state cities with no city-level entry all receive the
            # SAME state-level multiplier / difficulty (gold_standard
            # ``geo_basis`` "state"), so identical fingerprints are the
            # expected result of an area-level fallback -- not templated data.
            # Those rows are labelled as area-level estimates downstream, so
            # report them for what they are (informational) instead of
            # raising a medium "templated/duplicated data" alarm.
            bases = {
                str((city_data.get(c) or {}).get("geo_basis") or "") for c in cities
            }
            if bases and bases <= {"state", "country", "generic"}:
                areas = sorted(
                    {
                        str((city_data.get(c) or {}).get("geo_basis_area") or "")
                        for c in cities
                    }
                    - {""}
                )
                level = "/".join(sorted(bases))
                findings.append(
                    {
                        "check": "location_sanity",
                        "severity": "low",
                        "cities": cities,
                        "geo_basis": sorted(bases),
                        "message": (
                            f"Cities {cities} share one {level}-level estimate"
                            f"{' (' + ', '.join(areas) + ')' if areas else ''}: "
                            f"no city-specific salary/cost data on file. They are "
                            f"labelled as {level}-level estimates, not city data."
                        ),
                        "auto_corrected": False,
                    }
                )
                logger.info(
                    "Validator: %s share a %s-level fallback (labelled, not duplicated data)",
                    cities,
                    level,
                )
                continue
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


# ---------------------------------------------------------------------------
# Plan-integrity notes shared by the deck, the workbook and the validator
# ---------------------------------------------------------------------------

#: A plan whose overall data support is below this share is an internal draft.
INTERNAL_DRAFT_BELOW_PCT = 50


def data_confidence_pct(data: dict) -> int | None:
    """The plan's overall data-support score as a whole percent, from the same
    ``_synthesized.confidence_scores.overall`` the workbook grades. ``None`` when
    the plan carries no score (an absent score is not a low score)."""
    synthesized = data.get("_synthesized") if isinstance(data, dict) else None
    scores = (
        (synthesized or {}).get("confidence_scores")
        if isinstance(synthesized, dict)
        else None
    )
    if not isinstance(scores, dict):
        return None
    raw = scores.get("overall", scores.get("overall_confidence"))
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    return int(round(float(raw) * 100))


#: Core sections whose absence makes a plan unfit to send even when the average
#: over the sections that DID score is high (the overall score skips 0% ones).
_CORE_SECTIONS: tuple[tuple[str, str], ...] = (
    ("salary_intelligence", "salary"),
    ("job_market_demand", "job market demand"),
    ("location_profiles", "location"),
)


def _zero_core_sections(data: dict) -> list[str]:
    """Labels of the core sections the plan's own confidence scores put at 0%.
    A section with no score at all is not a 0% section (absent is not low)."""
    synthesized = data.get("_synthesized") if isinstance(data, dict) else None
    scores = (
        (synthesized or {}).get("confidence_scores")
        if isinstance(synthesized, dict)
        else None
    )
    if not isinstance(scores, dict):
        return []
    per_section = scores.get("sections", scores.get("per_section"))
    if not isinstance(per_section, dict):
        return []
    zero: list[str] = []
    for key, label in _CORE_SECTIONS:
        if key not in per_section:
            continue
        raw = per_section[key]
        if isinstance(raw, dict):
            raw = raw.get("score", raw.get("confidence"))
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            continue
        if int(round(float(raw) * 100)) == 0:
            zero.append(label)
    return zero


def internal_draft_stamp(data: dict) -> str:
    """ "Internal draft: data confidence 47%. Review before sharing with a
    client." when overall data support is below ``INTERNAL_DRAFT_BELOW_PCT``;
    when it is not but a core section (salary, demand, location) scores 0%,
    "Internal draft: <section> data missing (0%). Review before sharing with a
    client."; else ""."""
    pct = data_confidence_pct(data)
    if pct is not None and pct < INTERNAL_DRAFT_BELOW_PCT:
        return (
            f"Internal draft: data confidence {pct}%. "
            "Review before sharing with a client."
        )
    zero = _zero_core_sections(data)
    if zero:
        return (
            f"Internal draft: {', '.join(zero)} data missing (0%). "
            "Review before sharing with a client."
        )
    return ""


def _plan_cpa_in_usd(data: dict, cpa: float) -> tuple[float | None, str, str]:
    """(cpa in US$, currency code, "" or a reason it could not be converted).
    Compares in ONE currency using the rate the plan's own engine already
    carries (``metadata.intl_cpc_basis.usd_per_local``); with no rate the two
    figures are in different currencies and are not compared (declare, never
    convert)."""
    alloc = data.get("_budget_allocation") or {}
    meta = alloc.get("metadata") if isinstance(alloc, dict) else None
    basis = (meta or {}).get("intl_cpc_basis") if isinstance(meta, dict) else None
    basis = basis if isinstance(basis, dict) else {}
    code = str(basis.get("currency") or "").upper()
    if not code:
        try:
            import plan_currency

            code = str(plan_currency.currency_for_plan(data) or "USD").upper()
        except (ImportError, AttributeError, TypeError, ValueError):
            code = "USD"
    if code == "USD":
        return cpa, code, ""
    rate = basis.get("usd_per_local")
    if isinstance(rate, (int, float)) and not isinstance(rate, bool) and rate > 0:
        return cpa * float(rate), code, ""
    return None, code, "no exchange rate"


def _printed_country_cpa_range(data: dict) -> dict[str, Any] | None:
    """The CPA span the workbook's Intl Benchmarks sheet prints for a plan in
    ONE non-US country that has no industry-level range: the lowest low .. the
    highest high over every platform range printed for that country (US$), and
    which board carries the lowest low. None otherwise. It is a span of
    platform ranges, not a market or industry benchmark."""
    import publisher_filter

    if publisher_filter.plan_has_us_market(data):
        return None
    countries = publisher_filter.plan_countries(data)
    table = (data.get("_intl_benchmarks") or {}).get("countries")
    if len(countries) != 1 or not isinstance(table, dict):
        return None
    wanted = publisher_filter.canonical_country(countries[0])
    for key, entry in table.items():
        if not isinstance(entry, dict):
            continue
        name = entry.get("name", key)
        if publisher_filter.canonical_country(name) != wanted:
            continue
        lows: list[tuple[float, str]] = []
        highs: list[float] = []
        for plat in publisher_filter.clean_items(
            entry.get("platforms", []), data, country=name
        ):
            cpa = plat.get("cpa_usd") if isinstance(plat, dict) else None
            low = cpa.get("min", cpa.get("low")) if isinstance(cpa, dict) else None
            high = cpa.get("max", cpa.get("high")) if isinstance(cpa, dict) else None
            if (
                isinstance(low, (int, float))
                and isinstance(high, (int, float))
                and not isinstance(low, bool)
                and not isinstance(high, bool)
                and high > 0
            ):
                board = str(plat.get("name") or plat.get("platform") or "").strip()
                lows.append((float(low), board))
                highs.append(float(high))
        if lows:
            lowest, board = min(lows, key=lambda lb: lb[0])
            return {
                "country": countries[0],
                "low_usd": lowest,
                "high_usd": max(highs),
                "basis": "platforms",
                "lowest_board": board,
            }
    return None


def _channel_name(key: Any, ch: Any) -> str:
    """Client-facing channel name: the channel's own label, else the shared
    display name for its key (never "Programmatic Dsp" / "Emea Regional")."""
    import display_format

    label = str((ch or {}).get("label") or "") if isinstance(ch, dict) else ""
    return label or display_format.channel_label(str(key))


# --- the ONE CPA benchmark resolver -----------------------------------------
#
# The deck's benchmark slides, the workbook's benchmark note and the
# "optimistic" verdict all read the plan's CPA yardstick from
# :func:`resolve_cpa_benchmark` and nowhere else, so the verdict can never
# disagree with the range the deck prints.

# A US plan's yardstick is, in order: the knowledge-base row for the plan's own
# industry; else the cross-industry span of the ad-platform CPA ranges (the
# same range for every industry, so labelled as such).
CROSS_INDUSTRY_LABEL = "cross-industry ad platform range"
_INDUSTRY_LABEL = "indicative industry range"
_KB_CPA_SOURCE = "knowledge base: recruitment_benchmarks.industry_benchmarks"
_PLATFORM_CPA_SOURCE = "ad_platform_analysis (every platform's CPA range, all industries)"
_US_GAP_BASES = ("industry_us", "cross_industry_us")
_MARKET_SCOPES = ("market_industry", "market_platforms")

# A US plan has no per-country range. With only a median to go on, the plan must
# sit below this fraction of it before the note fires.
_US_MEDIAN_ONLY_FRACTION = 0.5


def _numbers_in(text: Any) -> list[float]:
    return [
        float(n.replace(",", ""))
        for n in re.findall(r"\d[\d,]*(?:\.\d+)?", str(text or ""))
        if n.replace(",", "")
    ]


def kb_extract_range(node: Any) -> str:
    """Pull a display-ready range/figure string out of one
    recruitment_benchmarks.industry_benchmarks[industry][metric] node.

    These KB nodes are free-form dicts: a "range" sub-key when present,
    otherwise the newest-looking numeric-year sub-key (e.g. "2025"), else
    "median"/"total_cost_per_hire", else the first string-valued sub-key
    found. Returns "" when nothing usable.

    ``recruitment_marketing_only`` (media spend alone) is never returned --
    not as a named key and not as the "first string" -- because a cost-per-
    hire cell is the all-in total or nothing; the ``cph`` node itself is read
    through ``budget_engine.kb_cph_node_text`` (the one shared selector), this
    function serves cpa / cpc / apply_rate.
    """
    if isinstance(node, str):
        return node
    if not isinstance(node, dict):
        return ""
    for key in ("range", "total_cost_per_hire", "median"):
        val = node.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    year_keys = [k for k in node if isinstance(k, str) and re.fullmatch(r"20\d{2}", k)]
    if year_keys:
        val = node.get(max(year_keys))
        if isinstance(val, str) and val.strip():
            return val.strip()
    for key, val in node.items():
        if key == "recruitment_marketing_only":
            continue
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def industry_cpa_reference(cpa_field: Any) -> dict[str, float | None] | None:
    """``{"low", "high", "median"}`` (US$; each None when absent) from the
    knowledge base's industry CPA field -- a ``{"range": "$12-$35", "median":
    "$20"}`` dict or a bare range string. None when it carries no figure."""
    if isinstance(cpa_field, dict):
        span = _numbers_in(cpa_field.get("range"))
        med = _numbers_in(cpa_field.get("median"))
    else:
        span, med = _numbers_in(cpa_field), []
    low = high = None
    if len(span) >= 2:
        low, high = min(span[:2]), max(span[:2])
    median = med[0] if med else None
    if low is None and median is None:
        return None
    return {"low": low, "high": high, "median": median}


def _kb_industry_cpa_node(data: dict, industry: str | None = None) -> Any:
    """The CPA field of the knowledge-base row for the plan's OWN industry key
    (the section the deck's and the workbook's benchmark tables read). None
    when the industry has no row -- there is no stand-in row: a general
    entry-level row would be one more industry's range printed as this one's."""
    kb = data.get("_knowledge_base")
    if not isinstance(kb, dict) or not kb:
        try:
            import kb_loader

            kb = kb_loader.load_knowledge_base()
        except Exception:  # noqa: BLE001 -- loader failures must not stop a plan
            logger.debug("industry benchmark load failed", exc_info=True)
            return None
    bench = kb.get("recruitment_benchmarks") if isinstance(kb, dict) else None
    table = bench.get("industry_benchmarks") if isinstance(bench, dict) else None
    if not isinstance(table, dict):
        return None
    row = table.get(industry or str(data.get("industry") or "general_entry_level"))
    return row.get("cpa") if isinstance(row, dict) else None


def _platform_cpa_span(data: dict) -> tuple[float, float] | None:
    """Lowest and highest CPA over every platform in the plan's synthesized
    ``ad_platform_analysis`` (whole dollars, as the deck prints them)."""
    synth = data.get("_synthesized")
    plats = synth.get("ad_platform_analysis") if isinstance(synth, dict) else None
    if not isinstance(plats, dict):
        return None
    vals: list[float] = []
    for name, plat in plats.items():
        if not isinstance(plat, dict) or str(name).startswith("_"):
            continue
        cpa = plat.get("avg_cpa") or plat.get("cpa")
        if isinstance(cpa, (int, float)) and not isinstance(cpa, bool) and cpa > 0:
            vals.append(float(cpa))
    if not vals:
        return None
    return float(round(min(vals)) or min(vals)), float(round(max(vals)) or max(vals))


def _usd_text(value: float) -> str:
    return f"${value:,.0f}" if float(value).is_integer() else f"${value:,.2f}"


def us_cpa_candidate(
    data: dict, cpa_field: Any = None, industry: str | None = None
) -> dict[str, Any] | None:
    """The US$ CPA yardstick for the plan's industry, whatever the plan's
    market: the knowledge-base row (``cpa_field`` when the caller already holds
    that row), else the cross-industry ad-platform span, labelled as such.
    Callers do not use this directly for a verdict: :func:`resolve_cpa_benchmark`
    withholds it from a plan with no US market."""
    if not isinstance(data, dict):
        return None
    node = cpa_field if cpa_field is not None else _kb_industry_cpa_node(data, industry)
    ref = industry_cpa_reference(node)
    if ref:
        by_range = ref["low"] is not None
        median = ref["median"]
        low = ref["low"] if by_range else float(median) * _US_MEDIAN_ONLY_FRACTION
        high = ref["high"] if by_range else float(median)
        return {
            "low": float(low),
            "high": float(high),
            "median": median,
            "currency": "USD",
            "basis_label": _INDUSTRY_LABEL,
            "source": _KB_CPA_SOURCE,
            "scope": "industry",
            "reference": "range" if by_range else "median",
            "country": "United States",
            "text": kb_extract_range(node),
        }
    span = _platform_cpa_span(data)
    if span is None:
        return None
    low, high = span
    return {
        "low": low,
        "high": high,
        "median": None,
        "currency": "USD",
        "basis_label": CROSS_INDUSTRY_LABEL,
        "source": _PLATFORM_CPA_SOURCE,
        "scope": "cross_industry_platforms",
        "reference": "range",
        "country": "United States",
        "text": _usd_text(low) if low == high else f"{_usd_text(low)} - {_usd_text(high)}",
    }


def _market_cpa_range(data: dict) -> dict[str, Any] | None:
    """The plan's own market range for a plan with no US market: its single
    non-US country's industry range, else the span of the platform ranges the
    workbook prints for that country (``basis`` "platforms")."""
    try:
        import intl_benchmark_lookup

        rng = intl_benchmark_lookup.get_plan_local_cpa_range(data)
        if rng:
            return {**rng, "basis": "industry"}
        return _printed_country_cpa_range(data)
    except (ImportError, AttributeError, KeyError, TypeError, ValueError):
        logger.debug("benchmark range lookup failed", exc_info=True)
        return None


def resolve_cpa_benchmark(
    data: dict, cpa_field: Any = None, industry: str | None = None
) -> dict[str, Any] | None:
    """THE CPA yardstick for a plan -- the one range the deck prints, the
    workbook's note quotes and the "optimistic" verdict tests against.

    ``{"low", "high", "median", "currency", "basis_label", "source", "scope"}``
    (US$; ``median`` None when absent), plus ``text`` (the range as printed),
    ``country`` and, for a market scope, ``basis`` / ``lowest_board``.

    * A plan with a US market: the knowledge-base row for its industry
      (``scope`` "industry"), else the cross-industry ad-platform span
      (``scope`` "cross_industry_platforms", ``basis_label`` says so).
    * A plan with no US market: its own country's range (``scope``
      "market_industry" / "market_platforms"), else None -- the US industry
      figure describes no market of that plan, so it is never a yardstick.
    * None when there is no figure at all. ``cpa_field`` is a knowledge-base CPA
      node the caller already holds (used only for a US plan).
    """
    if not isinstance(data, dict):
        return None
    import publisher_filter

    if publisher_filter.plan_has_us_market(data):
        return us_cpa_candidate(data, cpa_field, industry)
    rng = _market_cpa_range(data)
    if not rng:
        return None
    industry_basis = rng.get("basis") != "platforms"
    return {
        "low": float(rng["low_usd"]),
        "high": float(rng["high_usd"]),
        "median": rng.get("median_usd"),
        "currency": "USD",
        "basis_label": (
            "this market's industry benchmark range"
            if industry_basis
            else "platform CPA ranges printed for this market"
        ),
        "source": (
            "intl_benchmark_lookup.get_plan_local_cpa_range"
            if industry_basis
            else "_intl_benchmarks platform ranges"
        ),
        "scope": "market_industry" if industry_basis else "market_platforms",
        "country": rng["country"],
        "basis": rng.get("basis") or "industry",
        "lowest_board": rng.get("lowest_board") or "",
        "text": _usd_range_text(rng["low_usd"], rng["high_usd"]),
    }


def _market_gap(data: dict, bench: dict[str, Any]) -> dict[str, Any] | None:
    """The market-range verdict for ``bench`` (a market-scope resolution): the
    plan's blended cost per application (and each funded channel's) against it.

    ``status``: "below" (blended CPA under the low end), "channel_below" (blended
    inside the range but some channel under it), "within", or "not_comparable"
    (plan currency has no rate to US$, so the figures are not compared). Never
    changes a number; the callers only say so."""
    alloc = data.get("_budget_allocation") or {}
    if not isinstance(alloc, dict):
        return None
    total = alloc.get("total_projected") or {}
    cpa = total.get("cost_per_application") if isinstance(total, dict) else None
    if isinstance(cpa, bool) or not isinstance(cpa, (int, float)) or cpa <= 0:
        return None
    low = bench["low"]
    cpa_usd, code, why = _plan_cpa_in_usd(data, float(cpa))
    gap: dict[str, Any] = {
        "country": bench["country"],
        "low_usd": low,
        "high_usd": bench["high"],
        "basis": bench.get("basis") or "industry",
        "scope": bench["scope"],
        "lowest_board": bench.get("lowest_board") or "",
        "plan_cpa": float(cpa),
        "currency": code,
        "channels_below": [],
    }
    if cpa_usd is None:
        gap["status"] = "not_comparable"
        gap["reason"] = why
        return gap
    gap["plan_cpa_usd"] = cpa_usd
    for name, ch in (alloc.get("channel_allocations") or {}).items():
        if not isinstance(ch, dict) or (ch.get("dollar_amount") or 0) <= 0:
            continue
        ch_cpa = ch.get("cpa")
        if (
            isinstance(ch_cpa, (int, float))
            and not isinstance(ch_cpa, bool)
            and ch_cpa > 0
        ):
            ch_usd, _, _ = _plan_cpa_in_usd(data, float(ch_cpa))
            if ch_usd is not None and ch_usd < low:
                gap["channels_below"].append((_channel_name(name, ch), float(ch_cpa)))
    if cpa_usd < low:
        gap["status"] = "below"
    elif gap["channels_below"]:
        gap["status"] = "channel_below"
    else:
        gap["status"] = "within"
    return gap


def _plan_blended_cpa(data: dict) -> float | None:
    """The plan's blended cost per application: the engine's own total, else
    channel dollars over channel applications."""
    alloc = data.get("_budget_allocation")
    if not isinstance(alloc, dict):
        return None
    total = alloc.get("total_projected")
    cpa = total.get("cost_per_application") if isinstance(total, dict) else None
    if isinstance(cpa, (int, float)) and not isinstance(cpa, bool) and cpa > 0:
        return float(cpa)
    dollars = apps = 0.0
    chans = alloc.get("channel_allocations")
    for ch in (chans or {}).values() if isinstance(chans, dict) else ():
        if not isinstance(ch, dict):
            continue
        d = ch.get("dollar_amount", ch.get("dollars"))
        a = ch.get("projected_applications")
        dollars += float(d) if isinstance(d, (int, float)) else 0.0
        apps += float(a) if isinstance(a, (int, float)) else 0.0
    return dollars / apps if dollars > 0 and apps > 0 else None


def _us_gap(data: dict, bench: dict[str, Any]) -> dict[str, Any] | None:
    """The US-plan verdict: the blended CPA against ``bench`` (an industry or
    cross-industry resolution). Fires ("below") when the CPA is under the range
    low (for a median-only row, under half of it); "within" otherwise. Never
    compares a non-USD plan (declare, never convert). None with no CPA."""
    cpa = _plan_blended_cpa(data)
    if cpa is None:
        return None
    _, code, _ = _plan_cpa_in_usd(data, cpa)
    if code != "USD":
        return None
    low = bench["low"]
    return {
        "country": bench.get("country") or "United States",
        "basis": "industry_us" if bench["scope"] == "industry" else "cross_industry_us",
        "scope": bench["scope"],
        "reference": bench.get("reference") or "range",
        "low_usd": low,
        "high_usd": bench["high"],
        "median_usd": bench.get("median"),
        "threshold_usd": float(low),
        "plan_cpa": cpa,
        "plan_cpa_usd": cpa,
        "currency": "USD",
        "channels_below": [],
        "status": "below" if cpa < low else "within",
    }


def plan_cpa_gap(
    data: dict, cpa_field: Any = None, industry: str | None = None
) -> dict[str, Any] | None:
    """THE benchmark verdict both the deck and the workbook print: the plan's
    blended CPA against :func:`resolve_cpa_benchmark`'s one range. None when
    there is no range or no CPA to set against it."""
    bench = resolve_cpa_benchmark(data, cpa_field, industry)
    if bench is None:
        return None
    if bench["scope"] in _MARKET_SCOPES:
        return _market_gap(data, bench)
    return _us_gap(data, bench)


def cpa_benchmark_gap(data: dict) -> dict[str, Any] | None:
    """:func:`plan_cpa_gap` for a plan whose yardstick is its own market's range
    (a single non-US country); None for any other plan."""
    bench = resolve_cpa_benchmark(data)
    if bench is None or bench["scope"] not in _MARKET_SCOPES:
        return None
    return _market_gap(data, bench)


def industry_cpa_gap(
    data: dict, cpa_field: Any = None, industry: str | None = None
) -> dict[str, Any] | None:
    """:func:`plan_cpa_gap` for a plan whose yardstick is the US industry (or
    cross-industry) range; None for a plan with no US market."""
    bench = resolve_cpa_benchmark(data, cpa_field, industry)
    if bench is None or bench["scope"] in _MARKET_SCOPES:
        return None
    return _us_gap(data, bench)


def _industry_us_message(gap: dict[str, Any], short: bool) -> str:
    """The sentence for a US-plan gap (``industry_us`` / ``cross_industry_us``):
    names the figure, its currency (US$), what the range IS (an industry range,
    or the cross-industry platform span) and that the yardstick is a Nova
    working estimate."""
    cpa = f"${gap['plan_cpa']:,.2f}"
    low, high, med = gap["low_usd"], gap["high_usd"], gap.get("median_usd")
    cross = gap.get("basis") == "cross_industry_us"
    label = CROSS_INDUSTRY_LABEL if cross else _INDUSTRY_LABEL
    if gap.get("reference") == "range":
        rng = _usd_range_text(low, high)
        if short:
            return (
                f"Planned CPA {cpa} is below the Nova working estimate {label} "
                f"({rng}): optimistic until tested."
            )
        if med and not cross:
            tail = f"; the industry median (${med:,.2f}) is higher still"
        elif cross:
            tail = ""
        else:
            tail = f"; range midpoint (${(low + high) / 2:,.2f})"
        if cross:
            what = f"the low end of the {label} ({rng}; not specific to this industry)"
        else:
            what = f"the low end of the {label} (${low:,.2f}{tail})"
    else:
        if short:
            return (
                f"Planned CPA {cpa} is below half the Nova working estimate "
                f"industry median (US${med:,.0f}): optimistic until tested."
            )
        what = f"half of the indicative industry median (${med:,.2f})"
    return (
        f"Model vs. benchmark: this plan's blended CPA ({cpa}) sits below {what}. "
        f"The {'benchmark' if cross else 'industry'} figure is an unaudited Nova "
        "working estimate (US$); this plan's CPA is computed from its own channel "
        "mix and pricing. Treat it as optimistic until a test confirms it."
    )


def _usd_range_text(low: float, high: float) -> str:
    return f"US${low:,.0f}\u2013{high:,.0f}"


def _usd_low_text(low: float) -> str:
    """The lowest low of a platform span: cents below US$10, where rounding to
    a whole dollar could put the plan's CPA "inside" a figure it is under."""
    return f"US${low:,.2f}" if low < 10 else f"US${low:,.0f}"


def _gap_yardstick(gap: dict[str, Any], short: bool = False) -> str:
    """What the plan CPA is set against, worded for what the range IS: an
    industry range is "this market's industry benchmark range"; a span of the
    platform ranges the workbook prints is never called a market or industry
    benchmark (slide 6 says "Industry CPA: No local benchmark" for that plan)."""
    rng = _usd_range_text(gap["low_usd"], gap["high_usd"])
    if gap.get("basis") == "platforms":
        if short:
            return f"every platform CPA range printed for {gap['country']}"
        board = f"{gap['lowest_board']} " if gap.get("lowest_board") else ""
        return (
            f"every platform CPA range printed for {gap['country']} "
            f"(lowest: {board}{_usd_low_text(gap['low_usd'])})"
        )
    if short:
        return f"the industry benchmark ({rng})"
    return f"this market's industry benchmark range ({rng} per application)"


def benchmark_gap_fallback(gap: dict[str, Any] | None) -> str:
    """A one-line legend for ``benchmark_gap_message(short=True)`` when that
    sentence does not fit: chosen by the gap's status and basis, so it never
    says the blended CPA is below when only a channel is."""
    if not gap or gap.get("status") not in ("below", "channel_below"):
        return ""
    platforms = gap.get("basis") == "platforms"
    plain = (
        "the cross-industry platform range"
        if gap.get("basis") == "cross_industry_us"
        else "the industry range"
    )
    if gap["status"] == "below":
        target = "every printed platform CPA range" if platforms else plain
        return f"Planned CPA is below {target}: optimistic until tested."
    target = "every printed platform CPA range" if platforms else plain
    return f"One channel's planned CPA is below {target}: optimistic until tested."


def benchmark_gap_message(gap: dict[str, Any] | None, short: bool = False) -> str:
    """The one sentence both surfaces print for ``cpa_benchmark_gap``; "" when
    there is nothing to say. ``short`` is the one-line form for a slide legend."""
    if not gap or gap.get("status") in (None, "within"):
        return ""
    if gap.get("basis") in _US_GAP_BASES:
        return _industry_us_message(gap, short)
    try:
        import plan_currency

        symbol = plan_currency.symbol_for_code(gap["currency"])
    except (ImportError, AttributeError):
        symbol = ""
    if gap["status"] == "not_comparable":
        what = (
            "the platform CPA ranges printed for "
            f"{gap['country']} (US$, per application)"
            if gap.get("basis") == "platforms"
            else "this market's industry benchmark range "
            f"({_usd_range_text(gap['low_usd'], gap['high_usd'])} per application, US$)"
        )
        return (
            f"Planned cost per application ({symbol}{gap['plan_cpa']:,.2f}, "
            f"{gap['currency']}) and {what} are in different currencies and "
            "were not compared."
        )
    yardstick = _gap_yardstick(gap, short)
    if short:
        who = "Planned CPA" if gap["status"] == "below" else "A channel's planned CPA"
        amount = f" {symbol}{gap['plan_cpa']:,.2f}" if gap["status"] == "below" else ""
        return (
            f"{who}{amount} is below {yardstick}: optimistic until a test confirms it."
        )
    if gap["status"] == "below":
        return (
            f"Planned cost per application ({symbol}{gap['plan_cpa']:,.2f}) is below "
            f"{yardstick}. Treat it as optimistic until a test confirms it."
        )
    names = ", ".join(n for n, _ in gap["channels_below"][:3])
    return (
        f"Planned cost per application for {names} is below {yardstick}. "
        "Treat it as optimistic until a test confirms it."
    )


def _check_plan_integrity(data: dict) -> list[dict[str, Any]]:
    """Check 8: viability warnings the plan must not pass silently -- a cost per
    application below the market's own benchmark range, and overall data support
    below the internal-draft line. Warnings only; no number is changed."""
    findings: list[dict[str, Any]] = []
    gap = plan_cpa_gap(data)
    message = benchmark_gap_message(gap)
    if message:
        findings.append(
            {
                "check": "cpa_vs_benchmark",
                "severity": "medium",
                "message": message,
                "auto_corrected": False,
            }
        )
    stamp = internal_draft_stamp(data)
    if stamp:
        findings.append(
            {
                "check": "low_data_confidence",
                "severity": "medium",
                "message": stamp,
                "auto_corrected": False,
            }
        )
    return findings


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
        # Runs after the CPA check -- rescaling scales dollars and projections
        # together, so per-channel CPA/CPC ratios are preserved.
        ("budget_allocation_sum", _check_budget_allocation_sum),
        ("confidence_consistency", _check_confidence_consistency),
        ("hires_consistency", _check_hires_consistency),
        ("location_sanity", _check_location_sanity),
        ("plan_integrity", _check_plan_integrity),
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
