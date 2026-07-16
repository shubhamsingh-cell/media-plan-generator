#!/usr/bin/env python3
"""eval_gate.py -- CI quality gate for the media-plan generation stack.

Runs the in-repo evaluation harness (``eval_framework.EvalSuite``) and the
golden / red-team datasets under ``evals/`` and exits non-zero when quality
drops below configured thresholds.  Wiring this into CI means a model, prompt
or embedding change cannot *silently* regress plan quality -- the build turns
red instead.

Two independent gates are evaluated and combined:

  1.  **Absolute floors.**  The overall pass-rate and each category's
      pass-rate must clear a minimum bar.  Defaults are deliberately
      conservative (see ``DEFAULT_THRESHOLDS``) and can be overridden per
      run via CLI flags or a JSON config (``--config evals/thresholds.json``).

  2.  **Regression vs. a committed baseline.**  If a baseline file is present
      (``evals/baseline_scores.json`` by default) the gate fails when any
      score drops by more than ``max_regression`` percentage points below the
      last green run -- catching slow drift even while scores stay above the
      absolute floor.

The threshold logic lives in small, side-effect-free functions
(:func:`evaluate_gate`, :func:`_check_floors`, :func:`_check_regression`) so
it can be unit-tested against a *mocked* eval result without running the real
(slow) suite or touching the network.  See ``tests/test_eval_gate.py``.

Promptfoo datasets (``evals/promptfoo.yaml`` / ``evals/redteam.yaml``) require
Node + live API keys, so they are *not* executed here by default; their results
are folded in only when a ``--promptfoo-results <file>`` JSON export is passed
(produced by ``npx promptfoo eval --output ...``).  This keeps the gate fast,
deterministic and offline in CI while still letting a full run gate on the LLM
suite when results are available.

Usage::

    # Run the gate with built-in defaults (exit 0 = pass, 1 = fail, 2 = error)
    python3 scripts/eval_gate.py

    # Override the overall floor and write a machine-readable report
    python3 scripts/eval_gate.py --min-overall 85 --report evals/gate_report.json

    # Refresh the committed baseline after a verified-good run (no gating)
    python3 scripts/eval_gate.py --update-baseline

    # Fold in a Promptfoo JSON export and use a custom config
    python3 scripts/eval_gate.py --config evals/thresholds.json \
        --promptfoo-results evals/results.json

Exit codes:
    0  all gates passed
    1  one or more gates failed (regression / below floor)
    2  internal error (harness unavailable, bad config, etc.)

Python stdlib only.  Import-safe: importing this module has no side effects.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("eval_gate")

# Make the project root importable whether invoked as ``scripts/eval_gate.py``
# or ``python -m scripts.eval_gate`` from any cwd.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

EVALS_DIR = PROJECT_ROOT / "evals"
DEFAULT_BASELINE_PATH = EVALS_DIR / "baseline_scores.json"

# Exit codes (also exported for tests).
EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_ERROR = 2

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------
# ``min_category`` is keyed by the human-readable category label emitted by
# ``EvalSuite`` (e.g. "Budget Sanity").  A category absent from the map falls
# back to ``min_category_default``.  All values are pass-rate percentages.
DEFAULT_THRESHOLDS: Dict[str, Any] = {
    # Overall pass-rate across every case in every category.
    "min_overall": 90.0,
    # Default floor applied to any category not named explicitly below.
    "min_category_default": 80.0,
    # Per-category floors.  Labels match EvalSuite._CATEGORY_LABELS.
    "min_category": {
        "Budget Sanity": 90.0,
        "Collar Consistency": 85.0,
        "Geographic Coherence": 80.0,
        "CPA Reasonableness": 80.0,
    },
    # A run that exercised fewer than this many cases is treated as a gate
    # error (the harness silently degraded -- e.g. an import broke and a
    # whole category produced zero cases).
    "min_total_cases": 50,
    # Max allowed drop (percentage points) below the committed baseline for
    # the overall score or any single category before the gate fails.
    "max_regression": 3.0,
}


def load_thresholds(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """Return the effective thresholds, layering a JSON config over defaults.

    The config is shallow-merged so a partial file (e.g. only ``min_overall``)
    keeps the built-in defaults for everything else.  ``min_category`` is
    merged one level deeper so individual category floors can be tuned without
    redeclaring the whole map.

    A missing or unreadable config is *not* fatal -- defaults are used and a
    warning is logged -- so the gate never fails closed on a typo'd path.
    """
    thresholds: Dict[str, Any] = {
        **DEFAULT_THRESHOLDS,
        "min_category": dict(DEFAULT_THRESHOLDS["min_category"]),
    }
    if config_path is None:
        return thresholds
    try:
        raw = json.loads(Path(config_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.warning("Threshold config %s not found; using defaults", config_path)
        return thresholds
    except (OSError, ValueError) as exc:
        logger.warning(
            "Could not read threshold config %s (%s); using defaults", config_path, exc
        )
        return thresholds

    if not isinstance(raw, dict):
        logger.warning(
            "Threshold config %s is not a JSON object; using defaults", config_path
        )
        return thresholds

    for key, value in raw.items():
        if key == "min_category" and isinstance(value, dict):
            thresholds["min_category"].update(value)
        else:
            thresholds[key] = value
    return thresholds


# ---------------------------------------------------------------------------
# Pure threshold logic (unit-tested without the real suite)
# ---------------------------------------------------------------------------


def _category_scores(eval_result: Dict[str, Any]) -> Dict[str, float]:
    """Extract a {category_label: score_pct} map from a full-eval result.

    Accepts the shape returned by ``EvalSuite.run_full_eval`` -- either the
    top-level ``categories`` map or, as a fallback, the ``details`` sub-dicts.
    Always returns plain floats so downstream comparisons are total.
    """
    cats = eval_result.get("categories")
    if isinstance(cats, dict) and cats:
        return {str(k): float(v) for k, v in cats.items()}
    details = eval_result.get("details") or {}
    out: Dict[str, float] = {}
    for label, detail in details.items():
        if isinstance(detail, dict):
            out[str(label)] = float(detail.get("score_pct") or 0.0)
    return out


def _floor_for(label: str, thresholds: Dict[str, Any]) -> float:
    """Return the pass-rate floor for a category label."""
    per_cat = thresholds.get("min_category") or {}
    if label in per_cat:
        return float(per_cat[label])
    return float(thresholds.get("min_category_default", 0.0))


def _check_floors(
    eval_result: Dict[str, Any], thresholds: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Return a list of floor violations (empty == all floors cleared).

    Each violation is a dict: ``{kind, target, score, floor, message}``.
    """
    violations: List[Dict[str, Any]] = []

    # Guard: the harness must have actually run a meaningful number of cases.
    total_cases = int(eval_result.get("total_cases") or 0)
    min_cases = int(thresholds.get("min_total_cases") or 0)
    if total_cases < min_cases:
        violations.append(
            {
                "kind": "coverage",
                "target": "total_cases",
                "score": float(total_cases),
                "floor": float(min_cases),
                "message": (
                    f"Only {total_cases} eval cases ran (expected >= {min_cases}); "
                    "the harness likely degraded -- treating as a gate failure."
                ),
            }
        )

    # Overall floor.
    overall = float(eval_result.get("overall_score") or 0.0)
    min_overall = float(thresholds.get("min_overall", 0.0))
    if overall < min_overall:
        violations.append(
            {
                "kind": "floor",
                "target": "OVERALL",
                "score": overall,
                "floor": min_overall,
                "message": (
                    f"Overall score {overall:.1f}% is below the floor of "
                    f"{min_overall:.1f}%."
                ),
            }
        )

    # Per-category floors.
    for label, score in sorted(_category_scores(eval_result).items()):
        floor = _floor_for(label, thresholds)
        if score < floor:
            violations.append(
                {
                    "kind": "floor",
                    "target": label,
                    "score": score,
                    "floor": floor,
                    "message": (
                        f"Category '{label}' scored {score:.1f}%, below its floor "
                        f"of {floor:.1f}%."
                    ),
                }
            )
    return violations


def _check_regression(
    eval_result: Dict[str, Any],
    baseline: Optional[Dict[str, Any]],
    thresholds: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Return a list of regression violations vs. a committed baseline.

    A missing/empty baseline yields no violations (first run, nothing to
    regress against).  The baseline is expected to mirror the eval-result
    shape: an ``overall_score`` float and a ``categories`` map.  Only scores
    present in *both* current and baseline are compared, so adding a new
    category never spuriously fails the gate.
    """
    if not baseline:
        return []

    max_drop = float(thresholds.get("max_regression", 0.0))
    violations: List[Dict[str, Any]] = []

    cur_overall = float(eval_result.get("overall_score") or 0.0)
    base_overall = baseline.get("overall_score")
    if base_overall is not None:
        drop = float(base_overall) - cur_overall
        if drop > max_drop:
            violations.append(
                {
                    "kind": "regression",
                    "target": "OVERALL",
                    "score": cur_overall,
                    "baseline": float(base_overall),
                    "drop": round(drop, 2),
                    "max_drop": max_drop,
                    "message": (
                        f"Overall score dropped {drop:.1f} pts "
                        f"({float(base_overall):.1f}% -> {cur_overall:.1f}%), "
                        f"exceeding the allowed {max_drop:.1f} pt regression."
                    ),
                }
            )

    cur_cats = _category_scores(eval_result)
    base_cats = _category_scores(baseline)
    for label, cur_score in sorted(cur_cats.items()):
        if label not in base_cats:
            continue
        base_score = base_cats[label]
        drop = base_score - cur_score
        if drop > max_drop:
            violations.append(
                {
                    "kind": "regression",
                    "target": label,
                    "score": cur_score,
                    "baseline": base_score,
                    "drop": round(drop, 2),
                    "max_drop": max_drop,
                    "message": (
                        f"Category '{label}' dropped {drop:.1f} pts "
                        f"({base_score:.1f}% -> {cur_score:.1f}%), exceeding the "
                        f"allowed {max_drop:.1f} pt regression."
                    ),
                }
            )
    return violations


def evaluate_gate(
    eval_result: Dict[str, Any],
    thresholds: Dict[str, Any],
    baseline: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Apply all gates to an eval result and return a structured verdict.

    This is the single decision point -- :func:`main` only orchestrates I/O
    around it, which is why the unit tests exercise *this* function directly
    with a mocked ``eval_result``.

    Returns::

        {
            "passed": bool,
            "violations": [ {kind, target, message, ...}, ... ],
            "overall_score": float,
            "category_scores": {label: float},
            "total_cases": int,
        }
    """
    floor_violations = _check_floors(eval_result, thresholds)
    regression_violations = _check_regression(eval_result, baseline, thresholds)
    violations = floor_violations + regression_violations
    return {
        "passed": len(violations) == 0,
        "violations": violations,
        "overall_score": float(eval_result.get("overall_score") or 0.0),
        "category_scores": _category_scores(eval_result),
        "total_cases": int(eval_result.get("total_cases") or 0),
    }


# ---------------------------------------------------------------------------
# Promptfoo fold-in (optional, offline-safe)
# ---------------------------------------------------------------------------


def fold_in_promptfoo(
    eval_result: Dict[str, Any], promptfoo_path: Path
) -> Dict[str, Any]:
    """Merge a Promptfoo JSON export into the eval result as a category.

    Promptfoo's ``--output`` JSON exposes ``results.stats`` with ``successes``
    and ``failures`` counts.  We convert that to a pass-rate and add it as a
    synthetic "LLM Suite (promptfoo)" category, then recompute the overall
    score as a case-weighted average so the gate's floors/regressions apply to
    it uniformly.

    A missing or malformed file is logged and ignored (returns the input
    unchanged) -- the LLM suite is opt-in and must never hard-fail the gate by
    its mere absence.
    """
    try:
        raw = json.loads(Path(promptfoo_path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning(
            "Could not read promptfoo results %s (%s); skipping", promptfoo_path, exc
        )
        return eval_result

    stats = ((raw.get("results") or {}).get("stats")) or raw.get("stats") or {}
    successes = int(stats.get("successes") or 0)
    failures = int(stats.get("failures") or 0)
    pf_total = successes + failures
    if pf_total == 0:
        logger.warning("Promptfoo results %s had zero cases; skipping", promptfoo_path)
        return eval_result

    pf_score = round(successes / pf_total * 100.0, 2)
    label = "LLM Suite (promptfoo)"

    merged = dict(eval_result)
    merged_cats = dict(_category_scores(eval_result))
    merged_cats[label] = pf_score
    merged["categories"] = merged_cats

    # Recompute overall as a case-weighted average across native + promptfoo.
    native_cases = int(eval_result.get("total_cases") or 0)
    native_passed = int(eval_result.get("total_passed") or 0)
    total_cases = native_cases + pf_total
    total_passed = native_passed + successes
    merged["total_cases"] = total_cases
    merged["total_passed"] = total_passed
    merged["overall_score"] = (
        round(total_passed / total_cases * 100.0, 2) if total_cases else 0.0
    )
    logger.info(
        "Folded in promptfoo suite: %d/%d (%.1f%%)", successes, pf_total, pf_score
    )
    return merged


# ---------------------------------------------------------------------------
# Harness invocation + I/O
# ---------------------------------------------------------------------------


def run_native_eval() -> Dict[str, Any]:
    """Import and run the native ``EvalSuite``; raise on unavailability.

    Imported lazily so that simply *importing* this module (e.g. for unit
    testing the threshold logic) never pulls in budget_engine/trend_engine.
    """
    import eval_framework  # noqa: WPS433 (local import is intentional)

    suite = eval_framework.EvalSuite()
    return suite.run_full_eval()


def load_baseline(path: Path) -> Optional[Dict[str, Any]]:
    """Load the committed baseline scores, or ``None`` if absent/unreadable."""
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.info("No baseline at %s -- skipping regression check", path)
        return None
    except (OSError, ValueError) as exc:
        logger.warning(
            "Could not read baseline %s (%s); skipping regression", path, exc
        )
        return None


def write_baseline(path: Path, eval_result: Dict[str, Any]) -> None:
    """Persist the current scores as the new baseline (for ``--update-baseline``)."""
    payload = {
        "overall_score": float(eval_result.get("overall_score") or 0.0),
        "categories": _category_scores(eval_result),
        "total_cases": int(eval_result.get("total_cases") or 0),
        "total_passed": int(eval_result.get("total_passed") or 0),
    }
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    logger.info("Wrote baseline -> %s", path)


def format_report(verdict: Dict[str, Any]) -> str:
    """Render a human-readable summary of the gate verdict for CI logs."""
    lines = [
        "=" * 70,
        "  EVAL GATE",
        "=" * 70,
        f"  Overall: {verdict['overall_score']:.1f}%  "
        f"({verdict['total_cases']} cases)",
        "",
    ]
    for label, score in sorted(verdict["category_scores"].items()):
        lines.append(f"    {label:<32s} {score:6.1f}%")
    lines.append("")
    lines.append("-" * 70)
    if verdict["passed"]:
        lines.append("  RESULT: PASS -- all quality gates cleared.")
    else:
        lines.append(f"  RESULT: FAIL -- {len(verdict['violations'])} violation(s):")
        lines.append("")
        for v in verdict["violations"]:
            lines.append(f"    [{v['kind'].upper()}] {v['message']}")
    lines.append("=" * 70)
    return "\n".join(lines)


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CI quality gate for the media-plan eval harness.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="JSON file overriding threshold defaults (shallow-merged).",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=DEFAULT_BASELINE_PATH,
        help=f"Baseline scores JSON (default: {DEFAULT_BASELINE_PATH}).",
    )
    parser.add_argument(
        "--no-baseline",
        action="store_true",
        help="Skip the regression-vs-baseline check entirely.",
    )
    parser.add_argument(
        "--promptfoo-results",
        type=Path,
        default=None,
        help="Optional Promptfoo --output JSON to fold into the gate.",
    )
    parser.add_argument(
        "--min-overall",
        type=float,
        default=None,
        help="Override the overall pass-rate floor (percentage).",
    )
    parser.add_argument(
        "--max-regression",
        type=float,
        default=None,
        help="Override the max allowed regression (percentage points).",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Write the machine-readable JSON verdict to this path.",
    )
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="Run the suite and overwrite the baseline; does not gate.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress the human-readable report (still sets exit code).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point.  Returns a process exit code (see module docstring)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    args = _parse_args(argv)

    # 1. Run the native suite (the only step that can be slow / import-heavy).
    try:
        eval_result = run_native_eval()
    except Exception as exc:  # harness unavailable / crashed -> gate error
        logger.error("Eval harness failed to run: %s", exc, exc_info=True)
        return EXIT_ERROR

    # 2. Optionally fold in the Promptfoo LLM suite.
    if args.promptfoo_results is not None:
        eval_result = fold_in_promptfoo(eval_result, args.promptfoo_results)

    # 3. ``--update-baseline`` short-circuits gating: record and exit clean.
    if args.update_baseline:
        try:
            write_baseline(args.baseline, eval_result)
        except OSError as exc:
            logger.error("Failed to write baseline: %s", exc)
            return EXIT_ERROR
        return EXIT_PASS

    # 4. Assemble thresholds (config file + CLI overrides).
    thresholds = load_thresholds(args.config)
    if args.min_overall is not None:
        thresholds["min_overall"] = args.min_overall
    if args.max_regression is not None:
        thresholds["max_regression"] = args.max_regression

    # 5. Load the baseline (unless suppressed).
    baseline = None if args.no_baseline else load_baseline(args.baseline)

    # 6. The decision.
    verdict = evaluate_gate(eval_result, thresholds, baseline)

    # 7. Reporting.
    if not args.quiet:
        print(format_report(verdict))
    if args.report is not None:
        try:
            Path(args.report).parent.mkdir(parents=True, exist_ok=True)
            Path(args.report).write_text(
                json.dumps(verdict, indent=2, default=str) + "\n",
                encoding="utf-8",
            )
        except OSError as exc:
            logger.warning("Could not write report to %s: %s", args.report, exc)

    return EXIT_PASS if verdict["passed"] else EXIT_FAIL


if __name__ == "__main__":
    sys.exit(main())
