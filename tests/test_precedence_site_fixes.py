"""Behaviour tests for every site the precedence lint (test_precedence_lint.py)
found and fixed.

Each site had an ``or 0`` default attached to the wrong expression
(``X or 0 + Y`` parses as ``X or (0 + Y)``; ``a - b or 0`` defaults the
subtraction's RESULT, so a None operand raises ``TypeError``). One test class
per site; the docstring of each states what the pre-fix code actually did.

The shared-plan / plan-result sweep (the original bug) is covered separately in
test_cache_cleanup_ttl.py.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterator

import pytest

import api_enrichment
import api_portal
import app
import audit_tool
import eval_framework
import hire_signal
import market_pulse
import nova
from tests.sweep_driver import run_one_sweep

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TestApiPortalUsageCounters:
    """api_portal._maybe_flush_usage.

    Pre-fix: ``rec.get("total_requests") or 0 + 1`` == ``rec.get(...) or 1``.
    A new key became 1 and then never moved (an existing nonzero value is
    truthy, so ``or 1`` never ran): total_requests / total_errors were frozen.
    """

    @pytest.fixture
    def flush_env(self, monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[str, Any]]:
        store: dict[str, Any] = {"keys": {}, "usage_log": []}
        monkeypatch.setattr(api_portal, "_load_api_keys", lambda: store)
        monkeypatch.setattr(api_portal, "_save_api_keys", lambda data: None)
        monkeypatch.setattr(api_portal, "_last_flush_time", 0)
        with api_portal._usage_buffer_lock:
            saved = list(api_portal._usage_buffer)
            api_portal._usage_buffer.clear()
        try:
            yield store
        finally:
            with api_portal._usage_buffer_lock:
                api_portal._usage_buffer[:] = saved

    @staticmethod
    def _entry(error: bool) -> dict[str, Any]:
        return {
            "key_hash": "kh1",
            "timestamp": "2026-10-01T00:00:00Z",
            "endpoint": "/api/x",
            "error": error,
        }

    def test_counters_increment_from_existing_values(
        self, flush_env: dict[str, Any]
    ) -> None:
        flush_env["keys"]["kh1"] = {"total_requests": 5, "total_errors": 2}
        api_portal._usage_buffer.extend([self._entry(False), self._entry(True)])
        api_portal._maybe_flush_usage()
        rec = flush_env["keys"]["kh1"]
        assert rec["total_requests"] == 7
        assert rec["total_errors"] == 3

    @pytest.mark.parametrize("seed", [{}, {"total_requests": None}])
    def test_counters_start_from_zero(
        self, flush_env: dict[str, Any], seed: dict[str, Any]
    ) -> None:
        flush_env["keys"]["kh1"] = dict(seed)
        api_portal._usage_buffer.extend(
            [self._entry(False), self._entry(False), self._entry(False)]
        )
        api_portal._maybe_flush_usage()
        assert flush_env["keys"]["kh1"]["total_requests"] == 3


class TestMoveRegionalPct:
    """app._move_regional_pct (extracted from four inline sites).

    Pre-fix inline form: ``pcts.get("emea_regional") or 0 + apac`` ==
    ``pcts.get("emea_regional") or apac``. When EMEA already had a share the
    APAC share was popped and DROPPED, so the channel percentages no longer
    summed to the original total (budget silently lost on EMEA/APAC plans).
    """

    def test_adds_to_existing_destination_share(self) -> None:
        pcts = {"apac_regional": 10, "emea_regional": 15, "indeed": 75}
        moved = app._move_regional_pct(pcts, "apac_regional", "emea_regional")
        assert moved == 10
        assert pcts == {"emea_regional": 25, "indeed": 75}
        assert sum(pcts.values()) == 100

    def test_creates_destination_when_absent(self) -> None:
        pcts = {"apac_regional": 10, "indeed": 90}
        assert app._move_regional_pct(pcts, "apac_regional", "emea_regional") == 10
        assert pcts == {"emea_regional": 10, "indeed": 90}

    def test_none_destination_share_is_treated_as_zero(self) -> None:
        pcts = {"emea_regional": 12.5, "apac_regional": None, "x": 1}
        assert app._move_regional_pct(pcts, "emea_regional", "apac_regional") == 12.5
        assert pcts == {"apac_regional": 12.5, "x": 1}

    def test_absent_or_zero_source_is_a_noop(self) -> None:
        pcts = {"emea_regional": 15, "indeed": 85}
        assert app._move_regional_pct(pcts, "apac_regional", "emea_regional") == 0
        assert pcts == {"emea_regional": 15, "indeed": 85}
        pcts2 = {"apac_regional": 0, "emea_regional": 15}
        assert app._move_regional_pct(pcts2, "apac_regional", "emea_regional") == 0
        assert pcts2 == {"emea_regional": 15}

    def test_all_four_inline_sites_use_the_helper(self) -> None:
        """Two plan paths (sync + async) x two regions (emea, apac)."""
        src = (PROJECT_ROOT / "app.py").read_text(encoding="utf-8")
        assert src.count("_move_regional_pct(") == 1 + 4  # def + 4 call sites
        assert not re.search(
            r'channel_pcts\.get\("(emea|apac)_regional"\)\s*or 0 \+', src
        )


class TestHireSignalImpactScore:
    """hire_signal.generate_recommendations, 'Reduce' recommendation.

    Pre-fix: ``int((100 - q) * 0.8 + total_cost or 0 / 1000)`` added the RAW
    dollar cost (not cost/1000) to the score, so ``min(90, ...)`` pinned every
    low-quality source at impact_score 90; a source with no total_cost raised
    ``TypeError`` (float + None), failing the whole function.
    """

    @staticmethod
    def _impact(qoh: float, total_cost: Any) -> int:
        src = {
            "source": "Indeed",
            "avg_qoh_score": qoh,
            "total_hires": 3,
            "qoh_grade": "D",
            "quality_hire_pct": 10.0,
            "total_cost": total_cost,
        }
        recs = hire_signal.generate_recommendations(
            {"source_effectiveness": {"sources": {"Indeed": src}}}
        )
        reduce = [r for r in recs if r["category"] == "reduce"]
        assert len(reduce) == 1
        return reduce[0]["impact_score"]

    def test_impact_is_graded_not_saturated(self) -> None:
        # (100 - 40) * 0.8 + 25000 / 1000 = 73
        assert self._impact(40, 25000.0) == 73

    def test_more_cost_means_more_impact(self) -> None:
        assert self._impact(40, 5000.0) < self._impact(40, 25000.0) < 90

    def test_missing_cost_does_not_crash(self) -> None:
        # (100 - 40) * 0.8 + 0 = 48
        assert self._impact(40, None) == 48

    def test_impact_still_capped_at_90(self) -> None:
        assert self._impact(10, 500000.0) == 90


class TestMarketPulseZebraRows:
    """market_pulse.generate_pulse_html, platform comparison table.

    Pre-fix: ``BG_ZEBRA if p.get("rank") or 0 % 2 == 0 else "#ffffff"`` parses
    as ``rank or ((0 % 2) == 0)`` == ``rank or True`` -- always truthy, so every
    row got the zebra colour and the striping never alternated.
    """

    @staticmethod
    def _row_backgrounds(ranks: list[Any]) -> list[str]:
        platforms = [{"rank": r, "label": f"P{i}"} for i, r in enumerate(ranks)]
        html = market_pulse.generate_pulse_html(
            {"platform_shifts": {"available": True, "platforms": platforms}}
        )
        return re.findall(r'<tr style="background:([^;]+);">', html)

    def test_rows_alternate_by_rank_parity(self) -> None:
        assert self._row_backgrounds([1, 2, 3, 4]) == [
            "#ffffff",
            "#f4f4f9",
            "#ffffff",
            "#f4f4f9",
        ]

    def test_missing_rank_is_treated_as_zero_even(self) -> None:
        assert self._row_backgrounds([None]) == ["#f4f4f9"]


class TestNovaRoleDecompositionPercent:
    """nova.Nova._query_role_decomposition summary table.

    Pre-fix: ``{seg.get('pct_of_total') or 0*100:>9.0f}`` == ``pct or 0``.
    collar_intelligence returns pct_of_total as a 0-1 FRACTION, so a 30% segment
    printed as ``0%`` and a 100% segment as ``1%``.
    """

    class _StubCollar:
        @staticmethod
        def decompose_role(role: str, count: int, industry: str) -> list[dict]:
            return [
                {"title": "Junior X", "count": 3, "pct_of_total": 0.3},
                {"title": "Senior X", "count": 7, "pct_of_total": 0.7},
                {"title": "Lead X", "count": 0, "pct_of_total": None},
            ]

    def test_fraction_is_scaled_to_percent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(nova, "_get_collar_intel", lambda: self._StubCollar())
        out = nova.Nova._query_role_decomposition(
            None, {"role": "X", "count": 10, "industry": "tech"}
        )
        lines = out["summary_table"].splitlines()
        by_title = {ln.split()[0] + " " + ln.split()[1]: ln for ln in lines[2:]}
        assert re.search(r"\b30%", by_title["Junior X"])
        assert re.search(r"\b70%", by_title["Senior X"])
        assert re.search(r"\b0%", by_title["Lead X"])  # None -> 0, no crash


class TestBingMinMaxAverage:
    """api_enrichment._avg_min_max (extracted from three inline sites).

    Pre-fix: ``min.get(k) or 0 + max.get(k) or 0`` parses as
    ``min.get(k) or (0 + max.get(k)) or 0`` -- the Minimum alone (halved by the
    caller's ``/ 2.0``) instead of the Min/Max average.
    """

    def test_averages_min_and_max(self) -> None:
        assert api_enrichment._avg_min_max(
            {"AverageCpc": 1.0}, {"AverageCpc": 3.0}, "AverageCpc"
        ) == pytest.approx(2.0)

    def test_missing_or_none_side_counts_as_zero(self) -> None:
        assert api_enrichment._avg_min_max({}, {"Clicks": 10}, "Clicks") == 5.0
        assert (
            api_enrichment._avg_min_max({"Clicks": None}, {"Clicks": 10}, "Clicks")
            == 5.0
        )
        assert api_enrichment._avg_min_max({}, {}, "Clicks") == 0.0

    def test_all_three_inline_sites_use_the_helper(self) -> None:
        src = (PROJECT_ROOT / "api_enrichment.py").read_text(encoding="utf-8")
        assert src.count("_avg_min_max(") == 1 + 3  # def + 3 call sites


class TestAuditScorecardMissingBudget:
    """audit_tool.generate_audit_scorecard weighted efficiency score.

    Pre-fix: ``eff * ar.get("planned_budget") or 0`` -- the ``or 0`` defaulted the
    product, not the budget, so a result with planned_budget=None raised
    ``TypeError`` (``50 * None``) even though ``total_budget`` just above already
    tolerates None via ``or 0``.
    """

    def test_none_planned_budget_counts_as_zero_weight(self) -> None:
        card = audit_tool.generate_audit_scorecard(
            [
                {"channel": "A", "efficiency_score": 80, "planned_budget": 1000},
                {"channel": "B", "efficiency_score": 40, "planned_budget": None},
            ],
            [],
        )
        assert card["budget_efficiency_score"] == 80.0

    def test_weights_by_budget(self) -> None:
        card = audit_tool.generate_audit_scorecard(
            [
                {"channel": "A", "efficiency_score": 80, "planned_budget": 1000},
                {"channel": "B", "efficiency_score": 40, "planned_budget": 3000},
            ],
            [],
        )
        assert card["budget_efficiency_score"] == 50.0


class TestEvalBlueCollarChannelMix:
    """eval_framework blue-collar channel-mix check.

    Pre-fix: ``mix.get(a) or 0 + mix.get(b) or 0 + mix.get(c) or 0`` returned the
    FIRST truthy share alone (0.25), so the case reported '< 40%' and failed
    even though the three shares the comment says to sum are 0.25+0.20+0.40.
    """

    def test_blue_collar_mix_check_sums_the_shares(self) -> None:
        cases = {c["name"]: c for c in eval_framework._build_collar_cases()}
        ok, msg = cases["blue_collar_channel_mix_heavy"]["check"](None)
        assert ok is True, msg
        assert "85%" in msg


class TestCircuitBreakerMissingFailureTime:
    """api_enrichment._circuit_breaker_check.

    Pre-fix: ``time.time() - state.get("last_failure_time") or 0`` -- the
    ``or 0`` defaulted the elapsed RESULT; a state with no failure timestamp
    raised ``TypeError`` out of the breaker check.
    """

    KEY = "__precedence_test_api__"

    @pytest.fixture(autouse=True)
    def _clean_state(self) -> Iterator[None]:
        api_enrichment._circuit_breaker_state.pop(self.KEY, None)
        yield
        api_enrichment._circuit_breaker_state.pop(self.KEY, None)

    def test_missing_failure_time_half_opens_instead_of_raising(self) -> None:
        api_enrichment._circuit_breaker_state[self.KEY] = {
            "failure_count": 3,
            "last_failure_time": None,
            "is_open": True,
        }
        assert api_enrichment._circuit_breaker_check(self.KEY) is False
        assert api_enrichment._circuit_breaker_state[self.KEY]["is_open"] is False

    def test_recent_failure_keeps_circuit_open(self) -> None:
        api_enrichment._circuit_breaker_state[self.KEY] = {
            "failure_count": 3,
            "last_failure_time": api_enrichment.time.time() - 10,
            "is_open": True,
        }
        assert api_enrichment._circuit_breaker_check(self.KEY) is True

    def test_old_failure_half_opens(self) -> None:
        api_enrichment._circuit_breaker_state[self.KEY] = {
            "failure_count": 3,
            "last_failure_time": api_enrichment.time.time() - 10_000,
            "is_open": True,
        }
        assert api_enrichment._circuit_breaker_check(self.KEY) is False


class TestGenerationJobsSweep:
    """``app._cleanup_generation_jobs`` (sibling of the same defect class).

    Pre-fix: ``(now - jdata.get("created") or 0) > 600`` defaulted the
    subtraction's RESULT; a job with no ``created`` raised ``TypeError``, which
    the loop's ``except`` swallowed -- aborting the WHOLE sweep, so one
    malformed job blocked cleanup of every other job until it was removed.
    """

    @pytest.fixture
    def jobs(self) -> Iterator[dict[str, dict]]:
        with app._generation_jobs_lock:
            saved = dict(app._generation_jobs)
            app._generation_jobs.clear()
        try:
            yield app._generation_jobs
        finally:
            with app._generation_jobs_lock:
                app._generation_jobs.clear()
                app._generation_jobs.update(saved)

    def test_fresh_jobs_survive_and_old_ones_go(
        self, jobs: dict[str, dict], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        now = app.time.time()
        jobs["fresh-proc"] = {"status": "processing", "created": now - 30}
        jobs["fresh-done"] = {"status": "completed", "created": now - 30}
        jobs["old-done"] = {"status": "completed", "created": now - 700}
        run_one_sweep(monkeypatch, app._cleanup_generation_jobs)
        assert set(jobs) == {"fresh-proc", "fresh-done"}
        assert jobs["fresh-proc"]["status"] == "processing"

    def test_stuck_processing_job_is_failed_then_swept(
        self,
        jobs: dict[str, dict],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A processing job older than 10 min is marked failed and, being a
        failed job older than 10 min, is removed in the same pass."""
        jobs["stuck"] = {"status": "processing", "created": app.time.time() - 700}
        with caplog.at_level("WARNING", logger="app"):
            run_one_sweep(monkeypatch, app._cleanup_generation_jobs)
        assert "Marked stale job stuck as failed" in caplog.text
        assert "stuck" not in jobs

    def test_job_without_created_does_not_abort_the_sweep(
        self, jobs: dict[str, dict], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        now = app.time.time()
        jobs["malformed"] = {"status": "completed"}  # no "created"
        jobs["old-done"] = {"status": "completed", "created": now - 700}
        jobs["fresh-done"] = {"status": "completed", "created": now - 30}
        run_one_sweep(monkeypatch, app._cleanup_generation_jobs)
        # Missing timestamp is treated as stale, and the valid jobs are still
        # swept correctly in the same pass.
        assert set(jobs) == {"fresh-done"}
