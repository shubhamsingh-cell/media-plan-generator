"""
Regression Detection Framework for the Media Plan Budget Engine.

Runs a fixed set of reference scenarios through calculate_budget_allocation()
and compares the outputs to a persisted baseline snapshot.  Any metric that
drifts beyond a configurable threshold triggers an alert.

Usage -- CLI
------------
    # Create / overwrite the baseline from the current engine output:
    python regression_detector.py --save-baseline

    # Run a regression check against the saved baseline:
    python regression_detector.py --check

    # Run scenarios and print current results (no baseline comparison):
    python regression_detector.py --run

Usage -- as a library
---------------------
    from regression_detector import run_regression_check, compare_to_baseline

    current = run_regression_check()
    report  = compare_to_baseline(current)
    if report["total_alerts"] > 0:
        for alert in report["alerts"]:
            print(alert)

Alert thresholds (configurable via module constants):
    - CPA drift            > 10 %
    - Channel allocation   > 15 %
    - Hire projection      > 20 %

The baseline file lives at  data/persistent/regression_baseline.json
(relative to this module's parent directory).  The directory is created
automatically when save_baseline() is called for the first time.

This module uses only the Python standard library and never raises
uncaught exceptions -- every public function is wrapped in try/except so
that a broken scenario or missing dependency will produce a structured
error dict rather than a stack trace.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
_BASELINE_DIR = os.path.join(_MODULE_DIR, "data", "persistent")
_BASELINE_PATH = os.path.join(_BASELINE_DIR, "regression_baseline.json")

# ---------------------------------------------------------------------------
# Alert thresholds (percentage points)
# ---------------------------------------------------------------------------

THRESHOLD_CPA_DRIFT_PCT: float = 10.0
THRESHOLD_ALLOCATION_DRIFT_PCT: float = 15.0
THRESHOLD_HIRE_DRIFT_PCT: float = 20.0

# Severity labels for different drift magnitudes
_SEVERITY_LEVELS: List[Tuple[float, str]] = [
    (50.0, "critical"),
    (30.0, "high"),
    (15.0, "medium"),
    (0.0, "low"),
]

# ---------------------------------------------------------------------------
# Industry channel-percentage presets (mirrors app.py _INDUSTRY_ALLOC)
# ---------------------------------------------------------------------------
# Kept as a local copy so regression_detector.py is fully self-contained.

_CHANNEL_PRESETS: Dict[str, Dict[str, float]] = {
    "healthcare_medical": {
        "programmatic_dsp": 22,
        "global_boards": 15,
        "niche_boards": 30,
        "social_media": 10,
        "regional_boards": 10,
        "employer_branding": 8,
        "apac_regional": 3,
        "emea_regional": 2,
    },
    "tech_engineering": {
        "programmatic_dsp": 30,
        "global_boards": 15,
        "niche_boards": 20,
        "social_media": 18,
        "regional_boards": 5,
        "employer_branding": 7,
        "apac_regional": 3,
        "emea_regional": 2,
    },
    "retail_consumer": {
        "programmatic_dsp": 38,
        "global_boards": 22,
        "niche_boards": 8,
        "social_media": 20,
        "regional_boards": 7,
        "employer_branding": 3,
        "apac_regional": 1,
        "emea_regional": 1,
    },
    "logistics_supply_chain": {
        "programmatic_dsp": 35,
        "global_boards": 20,
        "niche_boards": 12,
        "social_media": 10,
        "regional_boards": 15,
        "employer_branding": 5,
        "apac_regional": 2,
        "emea_regional": 1,
    },
    "finance_banking": {
        "programmatic_dsp": 25,
        "global_boards": 18,
        "niche_boards": 25,
        "social_media": 10,
        "regional_boards": 7,
        "employer_branding": 10,
        "apac_regional": 3,
        "emea_regional": 2,
    },
    "hospitality_travel": {
        "programmatic_dsp": 38,
        "global_boards": 22,
        "niche_boards": 8,
        "social_media": 20,
        "regional_boards": 7,
        "employer_branding": 3,
        "apac_regional": 1,
        "emea_regional": 1,
    },
    "general_entry_level": {
        "programmatic_dsp": 35,
        "global_boards": 20,
        "niche_boards": 15,
        "social_media": 12,
        "regional_boards": 8,
        "employer_branding": 5,
        "apac_regional": 3,
        "emea_regional": 2,
    },
}

_DEFAULT_CHANNEL_PCT: Dict[str, float] = {
    "programmatic_dsp": 35,
    "global_boards": 20,
    "niche_boards": 15,
    "social_media": 12,
    "regional_boards": 8,
    "employer_branding": 5,
    "apac_regional": 3,
    "emea_regional": 2,
}

# ---------------------------------------------------------------------------
# 10 Reference Scenarios
# ---------------------------------------------------------------------------

REFERENCE_SCENARIOS: List[Dict[str, Any]] = [
    # 1. Healthcare + 50 nurses + Dallas, TX + $100,000
    {
        "name": "healthcare_nurses_dallas",
        "client_name": "HealthFirst Medical Group",
        "industry": "healthcare_medical",
        "budget": 100_000,
        "roles": [
            {"title": "Registered Nurse", "count": 50, "tier": "Clinical / Licensed"},
        ],
        "locations": [
            {"city": "Dallas", "state": "TX", "country": "US"},
        ],
    },
    # 2. Tech + 10 software engineers + San Francisco, CA + $200,000
    {
        "name": "tech_engineers_sf",
        "client_name": "NovaTech Solutions",
        "industry": "tech_engineering",
        "budget": 200_000,
        "roles": [
            {
                "title": "Software Engineer",
                "count": 10,
                "tier": "Professional / White-Collar",
            },
        ],
        "locations": [
            {"city": "San Francisco", "state": "CA", "country": "US"},
        ],
    },
    # 3. Retail + 200 store associates + nationwide + $500,000
    {
        "name": "retail_associates_nationwide",
        "client_name": "MegaMart Retail Corp",
        "industry": "retail_consumer",
        "budget": 500_000,
        "roles": [
            {"title": "Store Associate", "count": 200, "tier": "Hourly / Entry-Level"},
        ],
        "locations": [
            {"city": "Nationwide", "state": "", "country": "US"},
        ],
    },
    # 4. Logistics + 100 CDL drivers + Chicago, IL + $150,000
    {
        "name": "logistics_cdl_chicago",
        "client_name": "SwiftHaul Logistics",
        "industry": "logistics_supply_chain",
        "budget": 150_000,
        "roles": [
            {"title": "CDL Driver", "count": 100, "tier": "Skilled Trades / Technical"},
        ],
        "locations": [
            {"city": "Chicago", "state": "IL", "country": "US"},
        ],
    },
    # 5. Finance + 5 analysts + New York, NY + $50,000
    {
        "name": "finance_analysts_nyc",
        "client_name": "Pinnacle Capital Advisors",
        "industry": "finance_banking",
        "budget": 50_000,
        "roles": [
            {
                "title": "Financial Analyst",
                "count": 5,
                "tier": "Professional / White-Collar",
            },
        ],
        "locations": [
            {"city": "New York", "state": "NY", "country": "US"},
        ],
    },
    # 6. Hospitality + 50 hotel staff + Las Vegas, NV + $80,000
    {
        "name": "hospitality_hotelstaff_vegas",
        "client_name": "Grand Oasis Resorts",
        "industry": "hospitality_travel",
        "budget": 80_000,
        "roles": [
            {"title": "Hotel Staff", "count": 50, "tier": "Hourly / Entry-Level"},
        ],
        "locations": [
            {"city": "Las Vegas", "state": "NV", "country": "US"},
        ],
    },
    # 7. International: tech_engineering + 20 developers + London, UK + $120,000
    {
        "name": "international_tech_london",
        "client_name": "Codex Global Ltd",
        "industry": "tech_engineering",
        "budget": 120_000,
        "roles": [
            {
                "title": "Software Developer",
                "count": 20,
                "tier": "Professional / White-Collar",
            },
        ],
        "locations": [
            {"city": "London", "state": "", "country": "United Kingdom"},
        ],
    },
    # 8. Mixed collar: 30 warehouse workers + 10 data analysts + Houston, TX + $100,000
    {
        "name": "mixed_collar_houston",
        "client_name": "OmniCorp Distribution",
        "industry": "logistics_supply_chain",
        "budget": 100_000,
        "roles": [
            {"title": "Warehouse Worker", "count": 30, "tier": "Hourly / Entry-Level"},
            {
                "title": "Data Analyst",
                "count": 10,
                "tier": "Professional / White-Collar",
            },
        ],
        "locations": [
            {"city": "Houston", "state": "TX", "country": "US"},
        ],
    },
    # 9. Single role: 1 VP of Engineering + Boston, MA + $25,000
    {
        "name": "executive_single_boston",
        "client_name": "Apex Innovations Inc",
        "industry": "tech_engineering",
        "budget": 25_000,
        "roles": [
            {
                "title": "VP of Engineering",
                "count": 1,
                "tier": "Executive / Leadership",
            },
        ],
        "locations": [
            {"city": "Boston", "state": "MA", "country": "US"},
        ],
    },
    # 10. Zero budget edge case: retail + 50 store associates + Dallas, TX + $0
    {
        "name": "zero_budget_edge_case",
        "client_name": "ZeroBudget Test Co",
        "industry": "retail_consumer",
        "budget": 0,
        "roles": [
            {"title": "Store Associate", "count": 50, "tier": "Hourly / Entry-Level"},
        ],
        "locations": [
            {"city": "Dallas", "state": "TX", "country": "US"},
        ],
    },
]

# ---------------------------------------------------------------------------
# Lazy import of budget_engine
# ---------------------------------------------------------------------------

_budget_engine = None  # type: ignore[assignment]
_IMPORT_ERROR: Optional[str] = None


def _ensure_budget_engine() -> bool:
    """
    Lazily import budget_engine.  Returns True on success.

    Adds the module directory to sys.path if needed so that sibling
    module imports resolve correctly when this file is invoked standalone.
    """
    global _budget_engine, _IMPORT_ERROR
    if _budget_engine is not None:
        return True
    if _IMPORT_ERROR is not None:
        return False  # already tried and failed

    try:
        if _MODULE_DIR not in sys.path:
            sys.path.insert(0, _MODULE_DIR)
        import budget_engine as _be  # type: ignore[import-untyped]

        _budget_engine = _be
        return True
    except Exception as exc:
        _IMPORT_ERROR = f"Failed to import budget_engine: {exc}"
        logger.error(_IMPORT_ERROR)
        return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pct_change(baseline_val: float, current_val: float) -> float:
    """Return absolute percentage change between two values.

    Returns 0.0 when the baseline is zero (avoids division-by-zero).
    Both values of zero also returns 0.0 (no drift).
    """
    if baseline_val == 0.0:
        if current_val == 0.0:
            return 0.0
        return 100.0  # went from nothing to something
    return abs((current_val - baseline_val) / baseline_val) * 100.0


def _severity_for_drift(drift_pct: float) -> str:
    """Map a drift percentage to a human-readable severity label."""
    for threshold, label in _SEVERITY_LEVELS:
        if drift_pct >= threshold:
            return label
    return "low"


def _channel_pcts_for_industry(industry: str) -> Dict[str, float]:
    """Return a channel-percentage dict for the given industry key."""
    return dict(_CHANNEL_PRESETS.get(industry, _DEFAULT_CHANNEL_PCT))


def _extract_channel_allocation_pcts(
    channel_allocations: Dict[str, Any],
) -> Dict[str, float]:
    """Pull percentage values from a channel_allocations dict.

    The budget engine returns each channel as a sub-dict with a
    ``percentage`` key.  We normalise to a simple {channel: pct} map.
    """
    result: Dict[str, float] = {}
    try:
        for ch_name, ch_data in channel_allocations.items():
            if isinstance(ch_data, dict):
                result[ch_name] = round(float(ch_data.get("percentage", 0.0)), 2)
            else:
                result[ch_name] = 0.0
    except Exception:
        pass
    return result


def _total_projected_hires(result: Dict[str, Any]) -> int:
    """Safely extract total projected hires from a budget engine result."""
    try:
        tp = result.get("total_projected", {})
        return int(tp.get("hires") or 0)
    except Exception:
        return 0


def _total_projected_cpa(result: Dict[str, Any]) -> float:
    """Safely extract aggregate cost-per-application."""
    try:
        tp = result.get("total_projected", {})
        return float(tp.get("cost_per_application", 0.0))
    except Exception:
        return 0.0


def _collar_type_from_result(result: Dict[str, Any]) -> str:
    """Extract the collar type that was used for the allocation."""
    try:
        meta = result.get("metadata", {})
        return str(meta.get("collar_type_used", "unknown"))
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# Core public API
# ---------------------------------------------------------------------------


def run_scenario(scenario: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a single reference scenario through the budget engine.

    Parameters
    ----------
    scenario : dict
        A dict from REFERENCE_SCENARIOS (keys: name, client_name,
        industry, budget, roles, locations).

    Returns
    -------
    dict
        Normalised metrics::

            {
                "total_budget": float,
                "channel_allocations": {channel_name: pct, ...},
                "projected_cpa": float,
                "projected_hires": int,
                "collar_type": str,
                "raw_result": <full budget engine output>,
                "error": None | str,
            }

    This function never raises.  If anything goes wrong the ``error``
    key will contain a description and all numeric fields will be zero.
    """
    empty: Dict[str, Any] = {
        "total_budget": 0.0,
        "channel_allocations": {},
        "projected_cpa": 0.0,
        "projected_hires": 0,
        "collar_type": "unknown",
        "raw_result": {},
        "error": None,
    }

    # -- Pre-flight: can we even import the engine? --
    if not _ensure_budget_engine():
        empty["error"] = _IMPORT_ERROR or "budget_engine unavailable"
        return empty

    try:
        industry = scenario.get("industry", "general_entry_level")
        budget = float(scenario.get("budget") or 0)
        roles = list(scenario.get("roles") or [])
        locations = list(scenario.get("locations") or [])
        channel_pcts = _channel_pcts_for_industry(industry)

        result = _budget_engine.calculate_budget_allocation(
            total_budget=budget,
            roles=roles,
            locations=locations,
            industry=industry,
            channel_percentages=channel_pcts,
            synthesized_data=None,
            knowledge_base=None,
            collar_type="",
        )

        ch_allocs = _extract_channel_allocation_pcts(
            result.get("channel_allocations", {})
        )

        return {
            "total_budget": budget,
            "channel_allocations": ch_allocs,
            "projected_cpa": _total_projected_cpa(result),
            "projected_hires": _total_projected_hires(result),
            "collar_type": _collar_type_from_result(result),
            "raw_result": result,
            "error": None,
        }

    except Exception as exc:
        logger.error(
            "run_scenario failed for '%s': %s",
            scenario.get("name", "?"),
            traceback.format_exc(),
        )
        empty["error"] = f"{type(exc).__name__}: {exc}"
        return empty


def run_regression_check() -> Dict[str, Dict[str, Any]]:
    """
    Run ALL reference scenarios and collect their key metrics.

    Returns
    -------
    dict
        ``{scenario_name: {metric: value, ...}, ...}``

        Each scenario entry contains the fields produced by
        ``run_scenario()`` (minus ``raw_result`` to keep the snapshot
        JSON-serialisable and compact).

    This function never raises.
    """
    results: Dict[str, Dict[str, Any]] = {}

    for scenario in REFERENCE_SCENARIOS:
        name = scenario.get("name", "unnamed")
        try:
            outcome = run_scenario(scenario)
            # Strip the raw_result to keep the snapshot lean
            snapshot: Dict[str, Any] = {
                "total_budget": outcome.get("total_budget", 0.0),
                "channel_allocations": outcome.get("channel_allocations", {}),
                "projected_cpa": outcome.get("projected_cpa", 0.0),
                "projected_hires": outcome.get("projected_hires") or 0,
                "collar_type": outcome.get("collar_type", "unknown"),
                "error": outcome.get("error"),
            }
            results[name] = snapshot
        except Exception as exc:
            logger.error("run_regression_check: scenario '%s' failed: %s", name, exc)
            results[name] = {
                "total_budget": 0.0,
                "channel_allocations": {},
                "projected_cpa": 0.0,
                "projected_hires": 0,
                "collar_type": "unknown",
                "error": f"{type(exc).__name__}: {exc}",
            }

    return results


# ---------------------------------------------------------------------------
# Baseline persistence
# ---------------------------------------------------------------------------


def save_baseline(results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Persist regression results as the new baseline.

    Parameters
    ----------
    results : dict
        Output of ``run_regression_check()``.

    Returns
    -------
    dict
        ``{"saved": True, "path": str}`` on success or
        ``{"saved": False, "error": str}`` on failure.
    """
    try:
        os.makedirs(_BASELINE_DIR, exist_ok=True)
        payload = {
            "format_version": 1,
            "scenarios": results,
        }
        # Add a creation timestamp (stdlib only -- no datetime needed)
        try:
            import time

            payload["created_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        except Exception:
            pass

        with open(_BASELINE_PATH, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=False, default=str)

        logger.info("Baseline saved to %s", _BASELINE_PATH)
        return {"saved": True, "path": _BASELINE_PATH}

    except Exception as exc:
        msg = f"save_baseline failed: {exc}"
        logger.error(msg)
        return {"saved": False, "error": msg}


def load_baseline() -> Optional[Dict[str, Any]]:
    """
    Load a previously saved baseline snapshot.

    Returns
    -------
    dict or None
        The ``scenarios`` dict from the baseline file, or None if no
        baseline exists or parsing fails.
    """
    try:
        if not os.path.isfile(_BASELINE_PATH):
            logger.info("No baseline file found at %s", _BASELINE_PATH)
            return None
        with open(_BASELINE_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            logger.warning("Baseline file is not a dict; ignoring")
            return None
        return data.get("scenarios", data)
    except Exception as exc:
        logger.error("load_baseline failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Baseline comparison
# ---------------------------------------------------------------------------


def compare_to_baseline(current: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare current regression results against the saved baseline.

    Parameters
    ----------
    current : dict
        Output of ``run_regression_check()``.

    Returns
    -------
    dict ::

        {
            "alerts": [
                {
                    "scenario": str,
                    "metric": str,
                    "baseline": float,
                    "current": float,
                    "drift_pct": float,
                    "severity": str,  # "low" | "medium" | "high" | "critical"
                },
                ...
            ],
            "total_alerts": int,
            "scenarios_checked": int,
            "baseline_loaded": bool,
            "error": None | str,
        }

    This function never raises.
    """
    empty_report: Dict[str, Any] = {
        "alerts": [],
        "total_alerts": 0,
        "scenarios_checked": 0,
        "baseline_loaded": False,
        "error": None,
    }

    try:
        baseline = load_baseline()
        if baseline is None:
            empty_report["error"] = (
                "No baseline found.  Run with --save-baseline first."
            )
            return empty_report

        alerts: List[Dict[str, Any]] = []
        scenarios_checked = 0

        for scenario_name, cur_metrics in current.items():
            base_metrics = baseline.get(scenario_name)
            if base_metrics is None:
                # New scenario not in baseline -- skip, not a regression
                continue

            scenarios_checked += 1

            # Skip comparison if either side had an error
            if cur_metrics.get("error") or base_metrics.get("error"):
                if cur_metrics.get("error") and not base_metrics.get("error"):
                    alerts.append(
                        {
                            "scenario": scenario_name,
                            "metric": "execution_error",
                            "baseline": "success",
                            "current": str(cur_metrics["error"]),
                            "drift_pct": 100.0,
                            "severity": "critical",
                        }
                    )
                continue

            # -- CPA drift --
            _compare_scalar(
                alerts,
                scenario_name,
                "projected_cpa",
                float(base_metrics.get("projected_cpa") or 0),
                float(cur_metrics.get("projected_cpa") or 0),
                THRESHOLD_CPA_DRIFT_PCT,
            )

            # -- Hire projection drift --
            _compare_scalar(
                alerts,
                scenario_name,
                "projected_hires",
                float(base_metrics.get("projected_hires") or 0),
                float(cur_metrics.get("projected_hires") or 0),
                THRESHOLD_HIRE_DRIFT_PCT,
            )

            # -- Channel allocation drift (per channel) --
            base_allocs = base_metrics.get("channel_allocations", {})
            cur_allocs = cur_metrics.get("channel_allocations", {})
            all_channels = set(list(base_allocs.keys()) + list(cur_allocs.keys()))

            for channel in sorted(all_channels):
                base_pct = float(base_allocs.get(channel, 0.0))
                cur_pct = float(cur_allocs.get(channel, 0.0))
                _compare_scalar(
                    alerts,
                    scenario_name,
                    f"channel_allocation:{channel}",
                    base_pct,
                    cur_pct,
                    THRESHOLD_ALLOCATION_DRIFT_PCT,
                )

        return {
            "alerts": alerts,
            "total_alerts": len(alerts),
            "scenarios_checked": scenarios_checked,
            "baseline_loaded": True,
            "error": None,
        }

    except Exception as exc:
        logger.error("compare_to_baseline failed: %s", traceback.format_exc())
        empty_report["error"] = f"{type(exc).__name__}: {exc}"
        return empty_report


def _compare_scalar(
    alerts: List[Dict[str, Any]],
    scenario_name: str,
    metric_name: str,
    baseline_val: float,
    current_val: float,
    threshold_pct: float,
) -> None:
    """Append an alert to *alerts* if drift exceeds *threshold_pct*."""
    try:
        drift = _pct_change(baseline_val, current_val)
        if drift > threshold_pct:
            alerts.append(
                {
                    "scenario": scenario_name,
                    "metric": metric_name,
                    "baseline": round(baseline_val, 4),
                    "current": round(current_val, 4),
                    "drift_pct": round(drift, 2),
                    "severity": _severity_for_drift(drift),
                }
            )
    except Exception:
        pass  # never crash the comparison loop


# ---------------------------------------------------------------------------
# Pretty-printing utilities
# ---------------------------------------------------------------------------


def format_run_results(results: Dict[str, Dict[str, Any]]) -> str:
    """Return a human-readable summary of a regression run."""
    lines: List[str] = []
    lines.append("=" * 72)
    lines.append("  REGRESSION DETECTOR -- Current Run Results")
    lines.append("=" * 72)

    for name, metrics in results.items():
        lines.append("")
        lines.append(f"  Scenario: {name}")
        lines.append(f"  {'─' * 40}")

        if metrics.get("error"):
            lines.append(f"  ERROR: {metrics['error']}")
            continue

        lines.append(
            f"    Budget:           ${metrics.get('total_budget') or 0:>12,.2f}"
        )
        lines.append(
            f"    Projected CPA:    ${metrics.get('projected_cpa') or 0:>12,.2f}"
        )
        lines.append(
            f"    Projected Hires:   {metrics.get('projected_hires') or 0:>12,}"
        )
        lines.append(
            f"    Collar Type:       {metrics.get('collar_type', 'unknown'):>12}"
        )

        ch = metrics.get("channel_allocations", {})
        if ch:
            lines.append(f"    Channel Allocations:")
            for ch_name, pct in sorted(ch.items(), key=lambda x: -x[1]):
                lines.append(f"      {ch_name:<30} {pct:>6.1f}%")

    lines.append("")
    lines.append("=" * 72)
    return "\n".join(lines)


def format_comparison_report(report: Dict[str, Any]) -> str:
    """Return a human-readable comparison report."""
    lines: List[str] = []
    lines.append("=" * 72)
    lines.append("  REGRESSION DETECTOR -- Baseline Comparison Report")
    lines.append("=" * 72)

    if report.get("error"):
        lines.append(f"  ERROR: {report['error']}")
        lines.append("=" * 72)
        return "\n".join(lines)

    lines.append(f"  Scenarios checked: {report.get('scenarios_checked') or 0}")
    lines.append(f"  Total alerts:      {report.get('total_alerts') or 0}")
    lines.append("")

    alerts = report.get("alerts") or []
    if not alerts:
        lines.append("  No regressions detected. All metrics within thresholds.")
    else:
        # Group alerts by severity
        by_severity: Dict[str, List[Dict]] = {}
        for alert in alerts:
            sev = alert.get("severity", "low")
            by_severity.setdefault(sev, []).append(alert)

        for severity in ("critical", "high", "medium", "low"):
            sev_alerts = by_severity.get(severity, [])
            if not sev_alerts:
                continue
            lines.append(f"  [{severity.upper()}] ({len(sev_alerts)} alert(s))")
            lines.append(f"  {'─' * 40}")
            for a in sev_alerts:
                lines.append(f"    Scenario: {a['scenario']}")
                lines.append(f"    Metric:   {a['metric']}")
                lines.append(
                    f"    Baseline: {a['baseline']}  ->  Current: {a['current']}"
                )
                lines.append(f"    Drift:    {a['drift_pct']:.1f}%")
                lines.append("")

    lines.append("=" * 72)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> int:
    """CLI entry point.  Returns 0 on success, 1 on regression alerts."""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Regression detection for the media-plan budget engine.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--save-baseline",
        action="store_true",
        help="Run all scenarios and save results as the new baseline.",
    )
    group.add_argument(
        "--check",
        action="store_true",
        help="Run all scenarios and compare against the saved baseline.",
    )
    group.add_argument(
        "--run",
        action="store_true",
        help="Run all scenarios and print results (no comparison).",
    )

    args = parser.parse_args()

    try:
        current = run_regression_check()
    except Exception as exc:
        print(f"FATAL: run_regression_check raised: {exc}", file=sys.stderr)
        return 1

    if args.run:
        print(format_run_results(current))
        return 0

    if args.save_baseline:
        result = save_baseline(current)
        if result.get("saved"):
            print(f"Baseline saved to {result['path']}")
            print(format_run_results(current))
            return 0
        else:
            print(f"ERROR: {result.get('error', 'unknown')}", file=sys.stderr)
            return 1

    if args.check:
        report = compare_to_baseline(current)
        print(format_comparison_report(report))
        if report.get("error"):
            return 1
        if report.get("total_alerts") or 0 > 0:
            return 1
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
