"""Tests for scripts/eval_gate.py -- the CI eval quality gate.

Exercises the pure threshold logic (floors, regression, coverage guard),
the promptfoo fold-in, config merging, and the main() exit-code contract,
all with a *mocked* eval result so the real (slow, import-heavy) EvalSuite
never runs and no network/LLM/Supabase is touched.

Runs under pytest, or standalone: ``python3 tests/test_eval_gate.py``.
"""

import json
import sys
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import eval_gate  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures / builders
# ---------------------------------------------------------------------------


def _good_result(**overrides):
    """A full-eval result that clears the default thresholds."""
    result = {
        "categories": {
            "Budget Sanity": 95.0,
            "Collar Consistency": 90.0,
            "Geographic Coherence": 88.0,
            "CPA Reasonableness": 85.0,
        },
        "overall_score": 91.0,
        "total_cases": 120,
        "total_passed": 109,
        "details": {},
    }
    result.update(overrides)
    return result


def _defaults():
    return eval_gate.load_thresholds(None)


# ---------------------------------------------------------------------------
# Floor checks
# ---------------------------------------------------------------------------


def test_good_result_passes_all_gates():
    verdict = eval_gate.evaluate_gate(_good_result(), _defaults(), baseline=None)
    assert verdict["passed"] is True
    assert verdict["violations"] == []
    assert verdict["overall_score"] == 91.0
    assert verdict["total_cases"] == 120


def test_below_overall_floor_fails():
    result = _good_result(overall_score=80.0)  # default floor is 90
    verdict = eval_gate.evaluate_gate(result, _defaults(), baseline=None)
    assert verdict["passed"] is False
    floors = [v for v in verdict["violations"] if v["kind"] == "floor"]
    assert any(v["target"] == "OVERALL" for v in floors)


def test_below_category_floor_fails():
    result = _good_result()
    result["categories"]["Budget Sanity"] = 70.0  # floor is 90
    verdict = eval_gate.evaluate_gate(result, _defaults(), baseline=None)
    assert verdict["passed"] is False
    assert any(
        v["kind"] == "floor" and v["target"] == "Budget Sanity"
        for v in verdict["violations"]
    )


def test_unlisted_category_uses_default_floor():
    thresholds = _defaults()
    result = _good_result()
    # A category with no explicit floor falls back to min_category_default (80).
    result["categories"]["New Mystery Category"] = 75.0
    verdict = eval_gate.evaluate_gate(result, thresholds, baseline=None)
    assert verdict["passed"] is False
    assert any(v["target"] == "New Mystery Category" for v in verdict["violations"])

    result["categories"]["New Mystery Category"] = 82.0  # now above default floor
    verdict2 = eval_gate.evaluate_gate(result, thresholds, baseline=None)
    assert verdict2["passed"] is True


def test_low_case_count_is_a_gate_error():
    # The harness degraded -- only a handful of cases ran.
    result = _good_result(total_cases=10)
    verdict = eval_gate.evaluate_gate(result, _defaults(), baseline=None)
    assert verdict["passed"] is False
    assert any(v["kind"] == "coverage" for v in verdict["violations"])


def test_category_scores_fallback_to_details():
    # No top-level "categories" -- must derive from details[].score_pct.
    result = {
        "overall_score": 91.0,
        "total_cases": 120,
        "total_passed": 109,
        "categories": {},
        "details": {
            "Budget Sanity": {"score_pct": 95.0},
            "Collar Consistency": {"score_pct": 90.0},
            "Geographic Coherence": {"score_pct": 88.0},
            "CPA Reasonableness": {"score_pct": 85.0},
        },
    }
    scores = eval_gate._category_scores(result)
    assert scores["Budget Sanity"] == 95.0
    assert scores["CPA Reasonableness"] == 85.0


# ---------------------------------------------------------------------------
# Regression checks
# ---------------------------------------------------------------------------


def test_regression_within_tolerance_passes():
    baseline = {"overall_score": 92.0, "categories": {"Budget Sanity": 96.0}}
    result = _good_result(overall_score=91.0)  # 1pt drop, tolerance is 3pt
    verdict = eval_gate.evaluate_gate(result, _defaults(), baseline=baseline)
    assert verdict["passed"] is True


def test_overall_regression_beyond_tolerance_fails():
    baseline = {"overall_score": 95.0, "categories": {}}
    result = _good_result(overall_score=91.0)  # 4pt drop > 3pt tolerance
    verdict = eval_gate.evaluate_gate(result, _defaults(), baseline=baseline)
    assert verdict["passed"] is False
    regs = [v for v in verdict["violations"] if v["kind"] == "regression"]
    assert any(v["target"] == "OVERALL" and v["drop"] == 4.0 for v in regs)


def test_category_regression_beyond_tolerance_fails():
    baseline = {
        "overall_score": 91.0,
        "categories": {"Geographic Coherence": 95.0},
    }
    result = _good_result()  # Geographic Coherence is 88.0 -> 7pt drop
    verdict = eval_gate.evaluate_gate(result, _defaults(), baseline=baseline)
    assert verdict["passed"] is False
    assert any(
        v["kind"] == "regression" and v["target"] == "Geographic Coherence"
        for v in verdict["violations"]
    )


def test_new_category_absent_from_baseline_does_not_regress():
    baseline = {"overall_score": 91.0, "categories": {"Budget Sanity": 95.0}}
    result = _good_result()  # has 3 categories not in baseline
    verdict = eval_gate.evaluate_gate(result, _defaults(), baseline=baseline)
    # No category should be flagged as a regression for being new.
    assert all(
        not (v["kind"] == "regression" and v["target"] != "OVERALL")
        for v in verdict["violations"]
    )


def test_missing_baseline_skips_regression():
    result = _good_result(overall_score=10.0)  # would regress against anything
    verdict = eval_gate.evaluate_gate(result, _defaults(), baseline=None)
    # Floors still fire, but no regression violations exist with no baseline.
    assert all(v["kind"] != "regression" for v in verdict["violations"])


def test_improvement_never_flags_regression():
    baseline = {"overall_score": 80.0, "categories": {"Budget Sanity": 80.0}}
    result = _good_result()  # everything higher than baseline
    verdict = eval_gate.evaluate_gate(result, _defaults(), baseline=baseline)
    assert all(v["kind"] != "regression" for v in verdict["violations"])
    assert verdict["passed"] is True


# ---------------------------------------------------------------------------
# Threshold config merging
# ---------------------------------------------------------------------------


def test_load_thresholds_defaults_when_no_config():
    t = eval_gate.load_thresholds(None)
    assert t["min_overall"] == eval_gate.DEFAULT_THRESHOLDS["min_overall"]
    assert t["min_category"]["Budget Sanity"] == 90.0
    # Must be a copy -- mutating it must not corrupt the module defaults.
    t["min_category"]["Budget Sanity"] = 1.0
    assert eval_gate.DEFAULT_THRESHOLDS["min_category"]["Budget Sanity"] == 90.0


def test_load_thresholds_shallow_merges_config(tmp_path):
    cfg = tmp_path / "thresholds.json"
    cfg.write_text(
        json.dumps(
            {
                "min_overall": 95.0,
                "min_category": {"Budget Sanity": 99.0},
            }
        )
    )
    t = eval_gate.load_thresholds(cfg)
    assert t["min_overall"] == 95.0
    assert t["min_category"]["Budget Sanity"] == 99.0  # overridden
    # Untouched categories keep their default floors.
    assert t["min_category"]["CPA Reasonableness"] == 80.0
    # Unspecified top-level keys keep defaults.
    assert t["max_regression"] == eval_gate.DEFAULT_THRESHOLDS["max_regression"]


def test_load_thresholds_missing_file_falls_back_to_defaults(tmp_path):
    t = eval_gate.load_thresholds(tmp_path / "does_not_exist.json")
    assert t["min_overall"] == eval_gate.DEFAULT_THRESHOLDS["min_overall"]


def test_load_thresholds_bad_json_falls_back_to_defaults(tmp_path):
    cfg = tmp_path / "broken.json"
    cfg.write_text("{not valid json")
    t = eval_gate.load_thresholds(cfg)
    assert t["min_overall"] == eval_gate.DEFAULT_THRESHOLDS["min_overall"]


# ---------------------------------------------------------------------------
# Promptfoo fold-in
# ---------------------------------------------------------------------------


def test_fold_in_promptfoo_adds_category_and_reweights(tmp_path):
    pf = tmp_path / "results.json"
    pf.write_text(
        json.dumps(
            {
                "results": {"stats": {"successes": 9, "failures": 1}},
            }
        )
    )
    base = _good_result()  # 109/120 native
    merged = eval_gate.fold_in_promptfoo(base, pf)
    assert merged["categories"]["LLM Suite (promptfoo)"] == 90.0
    # Weighted overall = (109 + 9) / (120 + 10) = 118/130 = 90.77
    assert merged["total_cases"] == 130
    assert merged["total_passed"] == 118
    assert abs(merged["overall_score"] - 90.77) < 0.1


def test_fold_in_promptfoo_missing_file_is_noop(tmp_path):
    base = _good_result()
    merged = eval_gate.fold_in_promptfoo(base, tmp_path / "absent.json")
    assert merged == base


def test_fold_in_promptfoo_zero_cases_is_noop(tmp_path):
    pf = tmp_path / "empty.json"
    pf.write_text(json.dumps({"results": {"stats": {"successes": 0, "failures": 0}}}))
    base = _good_result()
    merged = eval_gate.fold_in_promptfoo(base, pf)
    assert merged == base


# ---------------------------------------------------------------------------
# Baseline I/O
# ---------------------------------------------------------------------------


def test_write_then_load_baseline_roundtrips(tmp_path):
    path = tmp_path / "evals" / "baseline_scores.json"
    eval_gate.write_baseline(path, _good_result())
    loaded = eval_gate.load_baseline(path)
    assert loaded["overall_score"] == 91.0
    assert loaded["categories"]["Budget Sanity"] == 95.0


def test_load_baseline_missing_returns_none(tmp_path):
    assert eval_gate.load_baseline(tmp_path / "nope.json") is None


# ---------------------------------------------------------------------------
# main() exit-code contract (native suite mocked -- no real eval run)
# ---------------------------------------------------------------------------


def test_main_returns_pass_on_good_scores():
    with mock.patch.object(eval_gate, "run_native_eval", return_value=_good_result()):
        rc = eval_gate.main(["--no-baseline", "--quiet"])
    assert rc == eval_gate.EXIT_PASS


def test_main_returns_fail_on_low_scores():
    bad = _good_result(overall_score=50.0)
    with mock.patch.object(eval_gate, "run_native_eval", return_value=bad):
        rc = eval_gate.main(["--no-baseline", "--quiet"])
    assert rc == eval_gate.EXIT_FAIL


def test_main_returns_error_when_harness_crashes():
    with mock.patch.object(
        eval_gate, "run_native_eval", side_effect=RuntimeError("import boom")
    ):
        rc = eval_gate.main(["--no-baseline", "--quiet"])
    assert rc == eval_gate.EXIT_ERROR


def test_main_cli_overrides_min_overall():
    result = _good_result(overall_score=91.0)
    with mock.patch.object(eval_gate, "run_native_eval", return_value=result):
        # Raise the floor above the actual score -> should now fail.
        rc = eval_gate.main(["--no-baseline", "--quiet", "--min-overall", "99"])
    assert rc == eval_gate.EXIT_FAIL


def test_main_update_baseline_writes_and_passes(tmp_path):
    path = tmp_path / "baseline.json"
    with mock.patch.object(eval_gate, "run_native_eval", return_value=_good_result()):
        rc = eval_gate.main(["--update-baseline", "--baseline", str(path), "--quiet"])
    assert rc == eval_gate.EXIT_PASS
    assert path.exists()
    assert eval_gate.load_baseline(path)["overall_score"] == 91.0


def test_main_writes_json_report(tmp_path):
    report = tmp_path / "gate_report.json"
    with mock.patch.object(eval_gate, "run_native_eval", return_value=_good_result()):
        rc = eval_gate.main(["--no-baseline", "--quiet", "--report", str(report)])
    assert rc == eval_gate.EXIT_PASS
    payload = json.loads(report.read_text())
    assert payload["passed"] is True
    assert "category_scores" in payload


def test_format_report_lists_violations():
    bad = _good_result(overall_score=50.0)
    verdict = eval_gate.evaluate_gate(bad, _defaults(), baseline=None)
    text = eval_gate.format_report(verdict)
    assert "FAIL" in text
    assert "[FLOOR]" in text
    assert "Overall score" in text


if __name__ == "__main__":
    _failures = 0
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            # Skip tmp_path-fixture tests in standalone mode.
            _argcount = _fn.__code__.co_argcount
            if _argcount:
                print(f"SKIP {_name} (needs pytest fixture)")
                continue
            try:
                _fn()
                print(f"PASS {_name}")
            except AssertionError as exc:
                _failures += 1
                print(f"FAIL {_name}: {exc}")
    sys.exit(1 if _failures else 0)
