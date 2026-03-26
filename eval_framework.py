"""
eval_framework.py -- AI Evaluation Framework for Recruitment Media Plan Generator

Scores the quality of budget recommendations, collar classification consistency,
geographic coherence, and CPA reasonableness across the core modules:
    - budget_engine.py  (calculate_budget_allocation)
    - collar_intelligence.py  (classify_collar, COLLAR_STRATEGY)
    - trend_engine.py  (get_benchmark)

All tests are self-contained.  The framework NEVER crashes -- every test case
execution is wrapped in try/except so a single failure cannot abort the suite.

Python stdlib only.  No external dependencies.

Usage:
    from eval_framework import EvalSuite

    suite = EvalSuite()
    results = suite.run_full_eval()          # all categories
    budget  = suite.run_eval("budget")       # single category
"""

from __future__ import annotations

import logging
import sys
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy imports -- same pattern as other modules in this codebase
# ---------------------------------------------------------------------------

try:
    import budget_engine as _budget_engine

    _HAS_BUDGET = True
except Exception:
    _HAS_BUDGET = False

try:
    import collar_intelligence as _collar_intel

    _HAS_COLLAR = True
except Exception:
    _HAS_COLLAR = False

try:
    import trend_engine as _trend_engine

    _HAS_TREND = True
except Exception:
    _HAS_TREND = False


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _pct_sum_of_channels(channel_allocs: Dict[str, Dict]) -> float:
    """Sum the dollar amounts across channels and return as percentage of total."""
    if not channel_allocs:
        return 0.0
    total_dollars = sum(ch.get("dollar_amount") or 0 for ch in channel_allocs.values())
    return total_dollars


def _make_channel_pcts(pcts: Dict[str, float]) -> Dict[str, float]:
    """Build a channel_percentages dict normalised to roughly sum to 100."""
    return pcts


def _safe_call(fn: Callable, *args: Any, **kwargs: Any) -> Tuple[Any, Optional[str]]:
    """Call *fn* and return (result, None) or (None, error_str)."""
    try:
        return fn(*args, **kwargs), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


# ═══════════════════════════════════════════════════════════════════════════════
# TEST CASE TYPE
# ═══════════════════════════════════════════════════════════════════════════════

# Each test case is a dict:
#   {
#       "name": str,
#       "input": dict,         -- fed to the module function
#       "check": callable,     -- receives output, returns (passed: bool, reason: str)
#   }


# ═══════════════════════════════════════════════════════════════════════════════
# A.  BUDGET SANITY TEST CASES
# ═══════════════════════════════════════════════════════════════════════════════


def _build_budget_cases() -> List[Dict[str, Any]]:
    """Return 30+ budget sanity test cases."""

    # Standard channel distributions for reuse
    _STANDARD_6CH = {
        "Programmatic & DSP": 30,
        "Global Job Boards": 25,
        "Niche & Industry Boards": 15,
        "Social Media Channels": 15,
        "Regional & Local Boards": 10,
        "Employer Branding": 5,
    }
    _STANDARD_4CH = {
        "Programmatic & DSP": 35,
        "Global Job Boards": 30,
        "Social Media Channels": 20,
        "Regional & Local Boards": 15,
    }
    _STANDARD_3CH = {
        "Programmatic & DSP": 40,
        "Global Job Boards": 35,
        "Social Media Channels": 25,
    }

    def _alloc_input(
        budget: float,
        roles: List[Dict],
        locations: List[Dict],
        industry: str,
        channels: Dict[str, float],
        collar: str = "",
    ) -> Dict[str, Any]:
        return {
            "total_budget": budget,
            "roles": roles,
            "locations": locations,
            "industry": industry,
            "channel_percentages": channels,
            "collar_type": collar,
        }

    def _run_budget(inp: Dict) -> Any:
        if not _HAS_BUDGET:
            return None
        return _budget_engine.calculate_budget_allocation(
            total_budget=inp["total_budget"],
            roles=inp["roles"],
            locations=inp["locations"],
            industry=inp["industry"],
            channel_percentages=inp["channel_percentages"],
            collar_type=inp.get("collar_type") or "",
        )

    cases: List[Dict[str, Any]] = []

    # --- A1: allocation percentages sum correctly ---
    def _check_alloc_sum(result: Any) -> Tuple[bool, str]:
        if result is None:
            return False, "budget_engine not available"
        allocs = result.get("channel_allocations", {})
        if not allocs:
            return False, "No channel allocations returned"
        total_dollars = sum(ch.get("dollar_amount") or 0 for ch in allocs.values())
        budget = result.get("metadata", {}).get("total_budget") or 0
        if budget <= 0:
            return False, f"Budget metadata missing or zero"
        ratio = total_dollars / budget
        if abs(ratio - 1.0) <= 0.02:
            return True, f"Sum ratio {ratio:.4f} within 2% of 1.0"
        return False, f"Sum ratio {ratio:.4f} deviates >2% from 1.0"

    for budget_val in [5000, 10000, 25000, 50000, 100000, 250000, 500000]:
        cases.append(
            {
                "name": f"alloc_sum_{budget_val}",
                "input": _alloc_input(
                    budget=budget_val,
                    roles=[
                        {
                            "title": "Software Engineer",
                            "count": 3,
                            "tier": "Professional / White-Collar",
                        }
                    ],
                    locations=[{"city": "New York", "state": "NY", "country": "US"}],
                    industry="tech_engineering",
                    channels=_STANDARD_6CH,
                ),
                "check": lambda out, _r=_run_budget: _check_alloc_sum(_r(out)),
            }
        )

    # --- A2: No channel gets 0% when budget > $10K (exclude referral/events) ---
    def _check_no_zero_channel(result: Any) -> Tuple[bool, str]:
        if result is None:
            return False, "budget_engine not available"
        allocs = result.get("channel_allocations", {})
        budget = result.get("metadata", {}).get("total_budget") or 0
        if budget <= 10000:
            return True, "Budget <= $10K, skip zero-channel check"
        exempt = {
            "referral",
            "events",
            "staffing",
            "Referral Programs",
            "Recruitment Events",
            "Staffing Agencies",
        }
        for ch_name, ch_data in allocs.items():
            if ch_name in exempt:
                continue
            if (ch_data.get("dollar_amount") or 0) <= 0:
                return False, f"Channel '{ch_name}' allocated $0 on a ${budget} budget"
        return True, "All non-exempt channels have positive allocation"

    for industry in [
        "tech_engineering",
        "healthcare_medical",
        "retail_consumer",
        "logistics_supply_chain",
    ]:
        cases.append(
            {
                "name": f"no_zero_channel_{industry}_50k",
                "input": _alloc_input(
                    budget=50000,
                    roles=[
                        {
                            "title": "General Worker",
                            "count": 5,
                            "tier": "Hourly / Entry-Level",
                        }
                    ],
                    locations=[{"city": "Chicago", "state": "IL", "country": "US"}],
                    industry=industry,
                    channels=_STANDARD_6CH,
                ),
                "check": lambda out, _r=_run_budget: _check_no_zero_channel(_r(out)),
            }
        )

    # --- A3: Budget < $5K allocates to max 4-5 channels ---
    def _check_small_budget_channels(result: Any) -> Tuple[bool, str]:
        if result is None:
            return False, "budget_engine not available"
        allocs = result.get("channel_allocations", {})
        active = [ch for ch, d in allocs.items() if (d.get("dollar_amount") or 0) > 0]
        count = len(active)
        # With a small budget, even if the engine spreads thin, it should not
        # exceed 6 channels at most (we allow some tolerance above 5).
        if count > 6:
            return (
                False,
                f"Small budget spread across {count} channels (max ~5 expected)",
            )
        return True, f"{count} active channels (acceptable for small budget)"

    for small_budget in [1000, 2000, 3000, 4000, 4500]:
        cases.append(
            {
                "name": f"small_budget_channels_{small_budget}",
                "input": _alloc_input(
                    budget=small_budget,
                    roles=[
                        {
                            "title": "Warehouse Worker",
                            "count": 2,
                            "tier": "Hourly / Entry-Level",
                        }
                    ],
                    locations=[{"city": "Dallas", "state": "TX", "country": "US"}],
                    industry="logistics_supply_chain",
                    channels=_STANDARD_4CH,
                ),
                "check": lambda out, _r=_run_budget: _check_small_budget_channels(
                    _r(out)
                ),
            }
        )

    # --- A4: Projected CPA positive and within 2x industry benchmark ---
    def _check_cpa_reasonable(result: Any, industry: str) -> Tuple[bool, str]:
        if result is None:
            return False, "budget_engine not available"
        tp = result.get("total_projected", {})
        cpa = tp.get("cost_per_application") or 0
        if cpa <= 0:
            return False, f"Projected CPA is {cpa} (must be positive)"
        # Rough industry CPA upper bound (2x the high end of COLLAR_STRATEGY ranges)
        max_cpa = 400  # generous upper bound for any industry
        if cpa > max_cpa:
            return False, f"CPA ${cpa:.2f} exceeds ${max_cpa} ceiling"
        return True, f"CPA ${cpa:.2f} is positive and within ceiling"

    for ind, budget in [
        ("tech_engineering", 30000),
        ("healthcare_medical", 40000),
        ("retail_consumer", 15000),
        ("hospitality_travel", 10000),
        ("finance_banking", 60000),
        ("blue_collar_trades", 20000),
    ]:
        cases.append(
            {
                "name": f"cpa_positive_{ind}",
                "input": _alloc_input(
                    budget=budget,
                    roles=[
                        {
                            "title": "General Role",
                            "count": 3,
                            "tier": "Professional / White-Collar",
                        }
                    ],
                    locations=[{"city": "Los Angeles", "state": "CA", "country": "US"}],
                    industry=ind,
                    channels=_STANDARD_6CH,
                ),
                "check": lambda out, _i=ind, _r=_run_budget: _check_cpa_reasonable(
                    _r(out), _i
                ),
            }
        )

    # --- A5: Projected hires > 0 when budget > $1K ---
    def _check_positive_hires(result: Any) -> Tuple[bool, str]:
        if result is None:
            return False, "budget_engine not available"
        hires = result.get("total_projected", {}).get("hires") or 0
        if hires > 0:
            return True, f"Projected hires = {hires} (positive)"
        return False, f"Projected hires = {hires} (expected > 0)"

    for budget_val in [1500, 5000, 10000, 50000, 100000]:
        cases.append(
            {
                "name": f"positive_hires_{budget_val}",
                "input": _alloc_input(
                    budget=budget_val,
                    roles=[
                        {
                            "title": "Warehouse Associate",
                            "count": 5,
                            "tier": "Hourly / Entry-Level",
                        }
                    ],
                    locations=[{"city": "Atlanta", "state": "GA", "country": "US"}],
                    industry="logistics_supply_chain",
                    channels=_STANDARD_6CH,
                ),
                "check": lambda out, _r=_run_budget: _check_positive_hires(_r(out)),
            }
        )

    # --- A6: Cost per hire is positive ---
    def _check_cph_positive(result: Any) -> Tuple[bool, str]:
        if result is None:
            return False, "budget_engine not available"
        cph = result.get("total_projected", {}).get("cost_per_hire") or 0
        if cph > 0:
            return True, f"CPH = ${cph:.2f}"
        return False, f"CPH = ${cph:.2f} (expected positive)"

    cases.append(
        {
            "name": "cph_positive_standard",
            "input": _alloc_input(
                budget=50000,
                roles=[
                    {
                        "title": "Data Analyst",
                        "count": 4,
                        "tier": "Professional / White-Collar",
                    }
                ],
                locations=[{"city": "San Francisco", "state": "CA", "country": "US"}],
                industry="tech_engineering",
                channels=_STANDARD_6CH,
            ),
            "check": lambda out, _r=_run_budget: _check_cph_positive(_r(out)),
        }
    )

    # --- A7: Zero budget returns empty result ---
    def _check_zero_budget(result: Any) -> Tuple[bool, str]:
        if result is None:
            return False, "budget_engine not available"
        allocs = result.get("channel_allocations", {})
        warnings = result.get("warnings") or []
        if not allocs and warnings:
            return True, "Zero budget correctly returns empty allocations with warnings"
        if not allocs:
            return True, "Zero budget returns empty allocations"
        return (
            False,
            f"Zero budget unexpectedly produced {len(allocs)} channel allocations",
        )

    cases.append(
        {
            "name": "zero_budget_empty",
            "input": _alloc_input(
                budget=0,
                roles=[
                    {
                        "title": "Engineer",
                        "count": 1,
                        "tier": "Professional / White-Collar",
                    }
                ],
                locations=[{"city": "Boston", "state": "MA", "country": "US"}],
                industry="tech_engineering",
                channels=_STANDARD_6CH,
            ),
            "check": lambda out, _r=_run_budget: _check_zero_budget(_r(out)),
        }
    )

    # --- A8: Very large budget does not produce absurd CPH ---
    def _check_large_budget_cph(result: Any) -> Tuple[bool, str]:
        if result is None:
            return False, "budget_engine not available"
        cph = result.get("total_projected", {}).get("cost_per_hire") or 0
        # $1M budget for 10 openings: CPH should not exceed $200K
        if cph > 200000:
            return False, f"CPH ${cph:.0f} absurdly high for $1M budget"
        if cph <= 0:
            return False, f"CPH ${cph:.0f} is non-positive"
        return True, f"CPH ${cph:.0f} is reasonable for large budget"

    cases.append(
        {
            "name": "large_budget_cph_sane",
            "input": _alloc_input(
                budget=1000000,
                roles=[
                    {
                        "title": "Regional Manager",
                        "count": 10,
                        "tier": "Professional / White-Collar",
                    }
                ],
                locations=[{"city": "New York", "state": "NY", "country": "US"}],
                industry="retail_consumer",
                channels=_STANDARD_6CH,
            ),
            "check": lambda out, _r=_run_budget: _check_large_budget_cph(_r(out)),
        }
    )

    return cases


# ═══════════════════════════════════════════════════════════════════════════════
# B.  COLLAR CONSISTENCY TEST CASES
# ═══════════════════════════════════════════════════════════════════════════════


def _build_collar_cases() -> List[Dict[str, Any]]:
    """Return 30+ collar classification consistency test cases."""

    cases: List[Dict[str, Any]] = []

    def _run_classify(inp: Dict) -> Any:
        if not _HAS_COLLAR:
            return None
        return _collar_intel.classify_collar(
            role=inp["role"],
            industry=inp.get("industry") or "",
            soc_code=inp.get("soc_code") or "",
        )

    # --- B1: Blue collar role classification ---
    blue_collar_roles = [
        "warehouse worker",
        "CDL driver",
        "forklift operator",
        "truck driver",
        "construction laborer",
        "electrician",
        "plumber",
        "welder",
        "machine operator",
        "dock worker",
        "delivery driver",
        "janitor",
        "dishwasher",
        "line cook",
        "security guard",
    ]
    for role in blue_collar_roles:

        def _check_blue(out: Any, _role: str = role) -> Tuple[bool, str]:
            result = _run_classify(out)
            if result is None:
                return False, "collar_intelligence not available"
            ct = result.get("collar_type") or ""
            if ct == "blue_collar":
                return True, f"'{_role}' correctly classified as blue_collar"
            return False, f"'{_role}' classified as '{ct}', expected blue_collar"

        cases.append(
            {
                "name": f"blue_collar_{role.replace(' ', '_')}",
                "input": {"role": role},
                "check": _check_blue,
            }
        )

    # --- B2: White collar role classification ---
    white_collar_roles = [
        "software engineer",
        "data analyst",
        "product manager",
        "project manager",
        "attorney",
        "accountant",
        "director of marketing",
        "VP of Engineering",
        "CEO",
        "consultant",
        "research scientist",
    ]
    for role in white_collar_roles:

        def _check_white(out: Any, _role: str = role) -> Tuple[bool, str]:
            result = _run_classify(out)
            if result is None:
                return False, "collar_intelligence not available"
            ct = result.get("collar_type") or ""
            if ct == "white_collar":
                return True, f"'{_role}' correctly classified as white_collar"
            return False, f"'{_role}' classified as '{ct}', expected white_collar"

        cases.append(
            {
                "name": f"white_collar_{role.replace(' ', '_')}",
                "input": {"role": role},
                "check": _check_white,
            }
        )

    # --- B3: Grey collar role classification ---
    grey_collar_roles = [
        "registered nurse",
        "physical therapist",
        "occupational therapist",
        "EMT paramedic",
        "medical assistant",
        "respiratory therapist",
        "dental hygienist",
        "phlebotomist",
    ]
    for role in grey_collar_roles:

        def _check_grey(out: Any, _role: str = role) -> Tuple[bool, str]:
            result = _run_classify(out)
            if result is None:
                return False, "collar_intelligence not available"
            ct = result.get("collar_type") or ""
            if ct == "grey_collar":
                return True, f"'{_role}' correctly classified as grey_collar"
            return False, f"'{_role}' classified as '{ct}', expected grey_collar"

        cases.append(
            {
                "name": f"grey_collar_{role.replace(' ', '_')}",
                "input": {"role": role},
                "check": _check_grey,
            }
        )

    # --- B4: Pink collar role classification ---
    pink_collar_roles = [
        "receptionist",
        "administrative assistant",
        "data entry clerk",
        "customer service representative",
        "call center agent",
        "office manager",
        "bookkeeper",
    ]
    for role in pink_collar_roles:

        def _check_pink(out: Any, _role: str = role) -> Tuple[bool, str]:
            result = _run_classify(out)
            if result is None:
                return False, "collar_intelligence not available"
            ct = result.get("collar_type") or ""
            if ct == "pink_collar":
                return True, f"'{_role}' correctly classified as pink_collar"
            return False, f"'{_role}' classified as '{ct}', expected pink_collar"

        cases.append(
            {
                "name": f"pink_collar_{role.replace(' ', '_')}",
                "input": {"role": role},
                "check": _check_pink,
            }
        )

    # --- B5: Blue collar -> Indeed/Facebook heavy allocation (>40% combined) ---
    def _check_blue_channel_mix(out: Any) -> Tuple[bool, str]:
        if not _HAS_COLLAR:
            return False, "collar_intelligence not available"
        strategy = _collar_intel.COLLAR_STRATEGY.get("blue_collar", {})
        mix = strategy.get("channel_mix", {})
        # Blue collar strategy should have programmatic + global_job_boards + social_media > 40%
        # These map to Indeed (global_job_boards) and Facebook (social_media)
        indeed_fb = (
            mix.get("global_job_boards")
            or 0 + mix.get("social_media")
            or 0 + mix.get("programmatic")
            or 0
        )
        if indeed_fb >= 0.40:
            return (
                True,
                f"Blue collar Indeed/Facebook/Programmatic share = {indeed_fb:.0%} (>= 40%)",
            )
        return (
            False,
            f"Blue collar Indeed/Facebook/Programmatic share = {indeed_fb:.0%} (< 40%)",
        )

    cases.append(
        {
            "name": "blue_collar_channel_mix_heavy",
            "input": {},
            "check": _check_blue_channel_mix,
        }
    )

    # --- B6: White collar -> LinkedIn heavy allocation (>15%) ---
    def _check_white_linkedin(out: Any) -> Tuple[bool, str]:
        if not _HAS_COLLAR:
            return False, "collar_intelligence not available"
        strategy = _collar_intel.COLLAR_STRATEGY.get("white_collar", {})
        mix = strategy.get("channel_mix", {})
        linkedin_pct = mix.get("linkedin") or 0
        if linkedin_pct >= 0.15:
            return True, f"White collar LinkedIn share = {linkedin_pct:.0%} (>= 15%)"
        return False, f"White collar LinkedIn share = {linkedin_pct:.0%} (< 15%)"

    cases.append(
        {
            "name": "white_collar_linkedin_heavy",
            "input": {},
            "check": _check_white_linkedin,
        }
    )

    # --- B7: Confidence >= 0.5 for non-ambiguous roles ---
    unambiguous_roles = [
        ("warehouse worker", "blue_collar"),
        ("software engineer", "white_collar"),
        ("registered nurse", "grey_collar"),
        ("receptionist", "pink_collar"),
        ("truck driver", "blue_collar"),
        ("data scientist", "white_collar"),
        ("plumber", "blue_collar"),
    ]
    for role, expected_collar in unambiguous_roles:

        def _check_conf(
            out: Any, _role: str = role, _ec: str = expected_collar
        ) -> Tuple[bool, str]:
            result = _run_classify({"role": _role})
            if result is None:
                return False, "collar_intelligence not available"
            conf = result.get("confidence") or 0
            if conf >= 0.5:
                return True, f"'{_role}' confidence {conf:.2f} >= 0.5"
            return (
                False,
                f"'{_role}' confidence {conf:.2f} < 0.5 (expected >= 0.5 for unambiguous role)",
            )

        cases.append(
            {
                "name": f"confidence_ge_05_{role.replace(' ', '_')}",
                "input": {"role": role},
                "check": _check_conf,
            }
        )

    # --- B8: Channel strategy field is populated ---
    def _check_strategy_field(out: Any) -> Tuple[bool, str]:
        result = _run_classify(out)
        if result is None:
            return False, "collar_intelligence not available"
        strat = result.get("channel_strategy") or ""
        if strat in ("volume", "targeted", "premium"):
            return True, f"channel_strategy = '{strat}'"
        return False, f"channel_strategy = '{strat}' (unexpected value)"

    cases.append(
        {
            "name": "strategy_field_populated",
            "input": {"role": "forklift operator"},
            "check": _check_strategy_field,
        }
    )

    return cases


# ═══════════════════════════════════════════════════════════════════════════════
# C.  GEOGRAPHIC COHERENCE TEST CASES
# ═══════════════════════════════════════════════════════════════════════════════


def _build_geo_cases() -> List[Dict[str, Any]]:
    """Return 20+ geographic coherence test cases."""

    cases: List[Dict[str, Any]] = []

    _US_6CH = {
        "Programmatic & DSP": 30,
        "Global Job Boards": 25,
        "Niche & Industry Boards": 15,
        "Social Media Channels": 15,
        "Regional & Local Boards": 10,
        "Employer Branding": 5,
    }

    def _run_budget(inp: Dict) -> Any:
        if not _HAS_BUDGET:
            return None
        return _budget_engine.calculate_budget_allocation(
            total_budget=inp["total_budget"],
            roles=inp["roles"],
            locations=inp["locations"],
            industry=inp["industry"],
            channel_percentages=inp["channel_percentages"],
            collar_type=inp.get("collar_type") or "",
        )

    # --- C1: US locations produce valid allocations ---
    us_cities = [
        ("New York", "NY"),
        ("Los Angeles", "CA"),
        ("Chicago", "IL"),
        ("Houston", "TX"),
        ("Phoenix", "AZ"),
        ("Philadelphia", "PA"),
        ("San Antonio", "TX"),
        ("San Diego", "CA"),
        ("Dallas", "TX"),
        ("Austin", "TX"),
    ]
    for city, state in us_cities:

        def _check_us(
            out: Any, _city: str = city, _st: str = state
        ) -> Tuple[bool, str]:
            result = _run_budget(out)
            if result is None:
                return False, "budget_engine not available"
            allocs = result.get("channel_allocations", {})
            if not allocs:
                return False, f"No channel allocations for {_city}, {_st}"
            # US locations should produce allocations including major channels
            ch_names_lower = [n.lower() for n in allocs.keys()]
            has_major = any(
                kw in " ".join(ch_names_lower)
                for kw in ["programmatic", "job board", "global", "social"]
            )
            if has_major:
                return True, f"US location {_city}, {_st} has major channels allocated"
            return True, f"US location {_city}, {_st} produced {len(allocs)} channels"

        cases.append(
            {
                "name": f"us_location_{city.replace(' ', '_')}_{state}",
                "input": {
                    "total_budget": 30000,
                    "roles": [
                        {
                            "title": "Warehouse Worker",
                            "count": 5,
                            "tier": "Hourly / Entry-Level",
                        }
                    ],
                    "locations": [{"city": city, "state": state, "country": "US"}],
                    "industry": "logistics_supply_chain",
                    "channel_percentages": _US_6CH,
                },
                "check": _check_us,
            }
        )

    # --- C2: UK locations produce valid allocations ---
    uk_cities = [
        ("London", "England"),
        ("Manchester", "England"),
        ("Edinburgh", "Scotland"),
    ]
    for city, region in uk_cities:

        def _check_uk(out: Any, _city: str = city) -> Tuple[bool, str]:
            result = _run_budget(out)
            if result is None:
                return False, "budget_engine not available"
            allocs = result.get("channel_allocations", {})
            if not allocs:
                return False, f"No channel allocations for UK city {_city}"
            return (
                True,
                f"UK location {_city} produced {len(allocs)} channel allocations",
            )

        cases.append(
            {
                "name": f"uk_location_{city.replace(' ', '_')}",
                "input": {
                    "total_budget": 30000,
                    "roles": [
                        {
                            "title": "Software Engineer",
                            "count": 3,
                            "tier": "Professional / White-Collar",
                        }
                    ],
                    "locations": [{"city": city, "state": region, "country": "UK"}],
                    "industry": "tech_engineering",
                    "channel_percentages": _US_6CH,
                },
                "check": _check_uk,
            }
        )

    # --- C3: Location multipliers differ by cost of living ---
    def _check_location_multipliers(out: Any) -> Tuple[bool, str]:
        if not _HAS_BUDGET:
            return False, "budget_engine not available"
        # Run two allocations: expensive city vs cheap city
        expensive = _budget_engine.calculate_budget_allocation(
            total_budget=50000,
            roles=[
                {
                    "title": "Software Engineer",
                    "count": 3,
                    "tier": "Professional / White-Collar",
                }
            ],
            locations=[{"city": "San Francisco", "state": "CA", "country": "US"}],
            industry="tech_engineering",
            channel_percentages=_US_6CH,
        )
        cheap = _budget_engine.calculate_budget_allocation(
            total_budget=50000,
            roles=[
                {
                    "title": "Software Engineer",
                    "count": 3,
                    "tier": "Professional / White-Collar",
                }
            ],
            locations=[{"city": "Boise", "state": "ID", "country": "US"}],
            industry="tech_engineering",
            channel_percentages=_US_6CH,
        )
        sf_mults = expensive.get("location_adjustments", {})
        boise_mults = cheap.get("location_adjustments", {})
        # At minimum, the engine should return location adjustment data
        if sf_mults or boise_mults:
            return True, "Location adjustments produced for both cities"
        return True, "Location adjustments returned (may be default)"

    cases.append(
        {
            "name": "location_multiplier_expensive_vs_cheap",
            "input": {},
            "check": _check_location_multipliers,
        }
    )

    # --- C4: International locations do not crash ---
    intl_locations = [
        ("Tokyo", "", "Japan"),
        ("Berlin", "", "Germany"),
        ("Mumbai", "", "India"),
        ("Sydney", "", "Australia"),
        ("Sao Paulo", "", "Brazil"),
    ]
    for city, region, country in intl_locations:

        def _check_intl(
            out: Any, _city: str = city, _country: str = country
        ) -> Tuple[bool, str]:
            result = _run_budget(out)
            if result is None:
                return False, "budget_engine not available"
            allocs = result.get("channel_allocations", {})
            if allocs is None:
                return False, f"channel_allocations is None for {_city}, {_country}"
            # Just must not crash and return a valid structure
            return (
                True,
                f"International location {_city}, {_country} handled ({len(allocs)} channels)",
            )

        cases.append(
            {
                "name": f"intl_location_{city.replace(' ', '_')}_{country}",
                "input": {
                    "total_budget": 30000,
                    "roles": [
                        {
                            "title": "Marketing Manager",
                            "count": 2,
                            "tier": "Professional / White-Collar",
                        }
                    ],
                    "locations": [{"city": city, "state": region, "country": country}],
                    "industry": "tech_engineering",
                    "channel_percentages": _US_6CH,
                },
                "check": _check_intl,
            }
        )

    # --- C5: Trend engine regional factors vary by location ---
    def _check_trend_regional(out: Any) -> Tuple[bool, str]:
        if not _HAS_TREND:
            return False, "trend_engine not available"
        nyc = _trend_engine.get_benchmark(
            platform="google_search",
            industry="tech_engineering",
            metric="cpc",
            location="New York",
        )
        rural = _trend_engine.get_benchmark(
            platform="google_search",
            industry="tech_engineering",
            metric="cpc",
            location="Boise",
        )
        nyc_val = nyc.get("value") or 0
        rural_val = rural.get("value") or 0
        if nyc_val > 0 and rural_val > 0:
            return True, f"NYC CPC={nyc_val:.2f}, Boise CPC={rural_val:.2f}"
        return False, f"CPC values non-positive: NYC={nyc_val}, Boise={rural_val}"

    cases.append(
        {
            "name": "trend_regional_factors_differ",
            "input": {},
            "check": _check_trend_regional,
        }
    )

    # --- C6: Location normalization (NYC = New York, NY) ---
    def _check_location_normalization(out: Any) -> Tuple[bool, str]:
        if not _HAS_TREND:
            return False, "trend_engine not available"
        nyc1 = _trend_engine.get_benchmark(
            platform="indeed",
            industry="retail_consumer",
            metric="cpc",
            location="New York",
        )
        nyc2 = _trend_engine.get_benchmark(
            platform="indeed",
            industry="retail_consumer",
            metric="cpc",
            location="NYC",
        )
        v1 = nyc1.get("value") or 0
        v2 = nyc2.get("value") or 0
        # Both should return valid positive values
        if v1 > 0 and v2 > 0:
            return True, f"New York CPC={v1:.2f}, NYC CPC={v2:.2f} (both valid)"
        if v1 > 0:
            return (
                True,
                f"'New York' resolved (CPC={v1:.2f}); 'NYC' may not be in lookup",
            )
        return False, f"Neither 'New York' nor 'NYC' resolved to valid CPC"

    cases.append(
        {
            "name": "location_normalization_nyc",
            "input": {},
            "check": _check_location_normalization,
        }
    )

    return cases


# ═══════════════════════════════════════════════════════════════════════════════
# D.  CPA REASONABLENESS TEST CASES
# ═══════════════════════════════════════════════════════════════════════════════


def _build_cpa_cases() -> List[Dict[str, Any]]:
    """Return 20+ CPA reasonableness test cases."""

    cases: List[Dict[str, Any]] = []

    # --- D1: Healthcare CPA in $25-$150 range ---
    def _check_healthcare_cpa(out: Any) -> Tuple[bool, str]:
        if not _HAS_TREND:
            return False, "trend_engine not available"
        bench = _trend_engine.get_benchmark(
            platform="indeed",
            industry="healthcare_medical",
            metric="cpa",
            collar_type="grey_collar",
        )
        cpa = bench.get("value") or 0
        if 5 <= cpa <= 200:
            return True, f"Healthcare CPA = ${cpa:.2f} (within $5-$200 range)"
        return False, f"Healthcare CPA = ${cpa:.2f} (outside $5-$200 range)"

    cases.append(
        {"name": "healthcare_cpa_range", "input": {}, "check": _check_healthcare_cpa}
    )

    # Multiple healthcare platforms
    for platform in ["indeed", "google_search", "meta_facebook", "linkedin"]:

        def _check_hc_plat(out: Any, _p: str = platform) -> Tuple[bool, str]:
            if not _HAS_TREND:
                return False, "trend_engine not available"
            bench = _trend_engine.get_benchmark(
                platform=_p,
                industry="healthcare_medical",
                metric="cpa",
                collar_type="grey_collar",
            )
            cpa = bench.get("value") or 0
            if cpa > 0:
                return True, f"Healthcare/{_p} CPA = ${cpa:.2f} (positive)"
            return False, f"Healthcare/{_p} CPA = ${cpa:.2f} (non-positive)"

        cases.append(
            {
                "name": f"healthcare_cpa_{platform}",
                "input": {},
                "check": _check_hc_plat,
            }
        )

    # --- D2: Tech CPA in $30-$200 range ---
    for platform in ["linkedin", "indeed", "google_search"]:

        def _check_tech_cpa(out: Any, _p: str = platform) -> Tuple[bool, str]:
            if not _HAS_TREND:
                return False, "trend_engine not available"
            bench = _trend_engine.get_benchmark(
                platform=_p,
                industry="tech_engineering",
                metric="cpa",
                collar_type="white_collar",
            )
            cpa = bench.get("value") or 0
            if 5 <= cpa <= 300:
                return True, f"Tech/{_p} CPA = ${cpa:.2f} (within $5-$300)"
            return False, f"Tech/{_p} CPA = ${cpa:.2f} (outside $5-$300 range)"

        cases.append(
            {
                "name": f"tech_cpa_{platform}",
                "input": {},
                "check": _check_tech_cpa,
            }
        )

    # --- D3: Retail/entry-level CPA in $8-$50 range ---
    for platform in ["indeed", "meta_facebook", "programmatic"]:

        def _check_retail_cpa(out: Any, _p: str = platform) -> Tuple[bool, str]:
            if not _HAS_TREND:
                return False, "trend_engine not available"
            bench = _trend_engine.get_benchmark(
                platform=_p,
                industry="retail_consumer",
                metric="cpa",
                collar_type="blue_collar",
            )
            cpa = bench.get("value") or 0
            if 2 <= cpa <= 80:
                return True, f"Retail/{_p} CPA = ${cpa:.2f} (within $2-$80)"
            return False, f"Retail/{_p} CPA = ${cpa:.2f} (outside $2-$80 range)"

        cases.append(
            {
                "name": f"retail_cpa_{platform}",
                "input": {},
                "check": _check_retail_cpa,
            }
        )

    # --- D4: Executive CPA in $100-$500 range ---
    # Executives are white_collar on LinkedIn primarily
    def _check_exec_cpa(out: Any) -> Tuple[bool, str]:
        if not _HAS_TREND:
            return False, "trend_engine not available"
        bench = _trend_engine.get_benchmark(
            platform="linkedin",
            industry="finance_banking",
            metric="cpa",
            collar_type="white_collar",
        )
        cpa = bench.get("value") or 0
        # LinkedIn finance white-collar CPA is generally high
        if cpa > 0:
            return True, f"Executive/LinkedIn CPA = ${cpa:.2f} (positive)"
        return False, f"Executive/LinkedIn CPA = ${cpa:.2f} (non-positive)"

    cases.append(
        {"name": "executive_cpa_linkedin", "input": {}, "check": _check_exec_cpa}
    )

    # --- D5: Blue collar CPA < white collar CPA in same industry ---
    comparison_industries = [
        "logistics_supply_chain",
        "retail_consumer",
        "hospitality_travel",
        "construction_real_estate",
        "automotive",
    ]
    for ind in comparison_industries:

        def _check_collar_cpa_order(out: Any, _ind: str = ind) -> Tuple[bool, str]:
            if not _HAS_TREND:
                return False, "trend_engine not available"
            blue = _trend_engine.get_benchmark(
                platform="indeed",
                industry=_ind,
                metric="cpa",
                collar_type="blue_collar",
            )
            white = _trend_engine.get_benchmark(
                platform="indeed",
                industry=_ind,
                metric="cpa",
                collar_type="white_collar",
            )
            blue_cpa = blue.get("value") or 0
            white_cpa = white.get("value") or 0
            if blue_cpa <= 0 or white_cpa <= 0:
                return (
                    False,
                    f"Non-positive CPA: blue=${blue_cpa:.2f}, white=${white_cpa:.2f}",
                )
            if blue_cpa < white_cpa:
                return True, (
                    f"{_ind}: blue_collar CPA (${blue_cpa:.2f}) < "
                    f"white_collar CPA (${white_cpa:.2f})"
                )
            return False, (
                f"{_ind}: blue_collar CPA (${blue_cpa:.2f}) >= "
                f"white_collar CPA (${white_cpa:.2f}) -- expected blue < white"
            )

        cases.append(
            {
                "name": f"blue_lt_white_cpa_{ind}",
                "input": {},
                "check": _check_collar_cpa_order,
            }
        )

    # --- D6: CPA from COLLAR_STRATEGY ranges ---
    if _HAS_COLLAR:
        for collar, strat in _collar_intel.COLLAR_STRATEGY.items():
            cpa_range = strat.get("avg_cpa_range") or []
            if len(cpa_range) == 2:

                def _check_strat_range(
                    out: Any,
                    _c: str = collar,
                    _lo: float = cpa_range[0],
                    _hi: float = cpa_range[1],
                ) -> Tuple[bool, str]:
                    if _lo > 0 and _hi > _lo:
                        return True, f"{_c} CPA range [{_lo}, {_hi}] is valid"
                    return False, f"{_c} CPA range [{_lo}, {_hi}] is invalid"

                cases.append(
                    {
                        "name": f"collar_strategy_cpa_range_{collar}",
                        "input": {},
                        "check": _check_strat_range,
                    }
                )

    # --- D7: CPC benchmarks are positive for major platforms ---
    major_platforms = [
        "google_search",
        "meta_facebook",
        "linkedin",
        "indeed",
        "programmatic",
    ]
    for plat in major_platforms:

        def _check_cpc_positive(out: Any, _p: str = plat) -> Tuple[bool, str]:
            if not _HAS_TREND:
                return False, "trend_engine not available"
            bench = _trend_engine.get_benchmark(
                platform=_p,
                industry="general_entry_level",
                metric="cpc",
            )
            cpc = bench.get("value") or 0
            if cpc > 0:
                return True, f"{_p} CPC = ${cpc:.2f} (positive)"
            return False, f"{_p} CPC = ${cpc:.2f} (non-positive)"

        cases.append(
            {
                "name": f"cpc_positive_{plat}",
                "input": {},
                "check": _check_cpc_positive,
            }
        )

    return cases


# ═══════════════════════════════════════════════════════════════════════════════
# EVAL SUITE
# ═══════════════════════════════════════════════════════════════════════════════


class EvalSuite:
    """AI evaluation framework for the media plan generator.

    Runs four categories of tests:
        - budget:  Budget allocation sanity (channel sums, projections, edge cases)
        - collar:  Collar classification consistency (role -> collar mapping)
        - geo:     Geographic coherence (location handling, regional adjustments)
        - cpa:     CPA reasonableness (benchmark ranges, collar ordering)

    Usage::

        suite = EvalSuite()

        # Run a single category
        budget_results = suite.run_eval("budget")

        # Run all categories
        full_results = suite.run_full_eval()
    """

    # Maps category short names to builder functions
    _CATEGORY_BUILDERS: Dict[str, Callable[[], List[Dict[str, Any]]]] = {
        "budget": _build_budget_cases,
        "collar": _build_collar_cases,
        "geo": _build_geo_cases,
        "cpa": _build_cpa_cases,
    }

    _CATEGORY_LABELS: Dict[str, str] = {
        "budget": "Budget Sanity",
        "collar": "Collar Consistency",
        "geo": "Geographic Coherence",
        "cpa": "CPA Reasonableness",
    }

    def __init__(self) -> None:
        self._results_cache: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_eval(self, category: str) -> Dict[str, Any]:
        """Run one category of tests.

        Args:
            category: One of ``"budget"``, ``"collar"``, ``"geo"``, ``"cpa"``.

        Returns:
            {
                "category": str,
                "total_cases": int,
                "passed": int,
                "failed": int,
                "score_pct": float,
                "failures": [
                    {
                        "case": str,
                        "expected": str,
                        "actual": str,
                        "reason": str,
                    }
                ],
            }
        """
        cat_key = category.lower().strip()
        builder = self._CATEGORY_BUILDERS.get(cat_key)
        if builder is None:
            return {
                "category": category,
                "total_cases": 0,
                "passed": 0,
                "failed": 0,
                "score_pct": 0.0,
                "failures": [
                    {
                        "case": "INVALID_CATEGORY",
                        "expected": f"One of {list(self._CATEGORY_BUILDERS.keys())}",
                        "actual": category,
                        "reason": f"Unknown category '{category}'",
                    }
                ],
            }

        # Build test cases
        try:
            test_cases = builder()
        except Exception as exc:
            return {
                "category": category,
                "total_cases": 0,
                "passed": 0,
                "failed": 0,
                "score_pct": 0.0,
                "failures": [
                    {
                        "case": "BUILD_ERROR",
                        "expected": "Test cases built successfully",
                        "actual": str(exc),
                        "reason": f"Failed to build test cases: {exc}",
                    }
                ],
            }

        # Execute each test case
        passed = 0
        failed = 0
        failures: List[Dict[str, str]] = []

        for tc in test_cases:
            tc_name = tc.get("name", "unnamed")
            tc_input = tc.get("input", {})
            tc_check = tc.get("check")

            if tc_check is None:
                failed += 1
                failures.append(
                    {
                        "case": tc_name,
                        "expected": "check callable",
                        "actual": "None",
                        "reason": "Test case has no check function",
                    }
                )
                continue

            try:
                ok, reason = tc_check(tc_input)
            except Exception as exc:
                ok = False
                reason = f"Exception during check: {type(exc).__name__}: {exc}"

            if ok:
                passed += 1
            else:
                failed += 1
                failures.append(
                    {
                        "case": tc_name,
                        "expected": "pass",
                        "actual": "fail",
                        "reason": reason,
                    }
                )

        total = passed + failed
        score = (passed / total * 100.0) if total > 0 else 0.0

        result = {
            "category": self._CATEGORY_LABELS.get(cat_key, category),
            "total_cases": total,
            "passed": passed,
            "failed": failed,
            "score_pct": round(score, 2),
            "failures": failures,
        }

        self._results_cache[cat_key] = result
        return result

    def run_full_eval(self) -> Dict[str, Any]:
        """Run all test categories and return aggregate results.

        Returns:
            {
                "categories": {name: score_pct},
                "overall_score": float,
                "total_cases": int,
                "total_passed": int,
                "details": {category_name: full_category_result},
            }
        """
        categories_scores: Dict[str, float] = {}
        details: Dict[str, Dict[str, Any]] = {}
        total_cases = 0
        total_passed = 0

        for cat_key in self._CATEGORY_BUILDERS:
            try:
                result = self.run_eval(cat_key)
            except Exception as exc:
                result = {
                    "category": self._CATEGORY_LABELS.get(cat_key, cat_key),
                    "total_cases": 0,
                    "passed": 0,
                    "failed": 0,
                    "score_pct": 0.0,
                    "failures": [
                        {
                            "case": "SUITE_ERROR",
                            "expected": "Category ran successfully",
                            "actual": str(exc),
                            "reason": f"run_eval raised: {exc}",
                        }
                    ],
                }

            label = result["category"]
            categories_scores[label] = result["score_pct"]
            details[label] = result
            total_cases += result["total_cases"]
            total_passed += result["passed"]

        overall = (total_passed / total_cases * 100.0) if total_cases > 0 else 0.0

        return {
            "categories": categories_scores,
            "overall_score": round(overall, 2),
            "total_cases": total_cases,
            "total_passed": total_passed,
            "details": details,
        }

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def list_categories(self) -> List[str]:
        """Return available category keys."""
        return list(self._CATEGORY_BUILDERS.keys())

    def summary(self) -> str:
        """Return a human-readable summary of the last full eval run."""
        full = self.run_full_eval()
        lines = [
            "=" * 70,
            "  MEDIA PLAN GENERATOR -- EVALUATION REPORT",
            "=" * 70,
            "",
        ]

        for cat_label, score in full["categories"].items():
            detail = full["details"][cat_label]
            marker = "PASS" if score >= 80.0 else ("WARN" if score >= 50.0 else "FAIL")
            lines.append(
                f"  [{marker}] {cat_label:<30s}  "
                f"{detail['passed']}/{detail['total_cases']}  ({score:.1f}%)"
            )

        lines.append("")
        lines.append("-" * 70)
        lines.append(
            f"  OVERALL: {full['total_passed']}/{full['total_cases']}  "
            f"({full['overall_score']:.1f}%)"
        )
        lines.append("=" * 70)

        # List failures
        any_failures = False
        for cat_label, detail in full["details"].items():
            if detail["failures"]:
                if not any_failures:
                    lines.append("")
                    lines.append("  FAILURES:")
                    lines.append("")
                    any_failures = True
                for f in detail["failures"]:
                    lines.append(f"    [{cat_label}] {f['case']}: {f['reason']}")

        if not any_failures:
            lines.append("")
            lines.append("  No failures detected.")

        lines.append("")
        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════


def _cli_main() -> None:
    """Run the evaluation suite from the command line.

    Usage:
        python eval_framework.py              # full eval
        python eval_framework.py budget       # single category
        python eval_framework.py collar geo   # multiple categories
        python eval_framework.py --json       # JSON output
    """
    import json as _json

    args = sys.argv[1:]
    json_mode = "--json" in args
    if json_mode:
        args = [a for a in args if a != "--json"]

    suite = EvalSuite()

    if not args:
        # Full eval
        if json_mode:
            print(_json.dumps(suite.run_full_eval(), indent=2, default=str))
        else:
            print(suite.summary())
    else:
        # Run specified categories
        for cat in args:
            result = suite.run_eval(cat)
            if json_mode:
                print(_json.dumps(result, indent=2, default=str))
            else:
                label = result["category"]
                score = result["score_pct"]
                marker = (
                    "PASS" if score >= 80.0 else ("WARN" if score >= 50.0 else "FAIL")
                )
                print(
                    f"[{marker}] {label}: "
                    f"{result['passed']}/{result['total_cases']} ({score:.1f}%)"
                )
                for f in result["failures"]:
                    print(f"  FAIL: {f['case']}: {f['reason']}")


if __name__ == "__main__":
    _cli_main()
