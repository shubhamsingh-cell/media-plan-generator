"""Regression: the budget-sufficiency check grades the plan against the
client's real hiring target, and its "realistic hires" is the plan's own
projection (audit 2026-10-01, F §3.6 / §4.8).

Pre-fix (f99beef): both /api/generate paths build role dicts with
``count: 1`` per title, so ``total_openings`` was 1 for a one-role plan
and the check printed "WELL-FUNDED: Budget of $250,000/hire exceeds the
industry average", ``target_hires: 1`` and ``realistic_hires: 71`` next to
``total_projected.hires: 47``. The wizard's ``hire_volume`` was never
passed.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402
import budget_engine as be  # noqa: E402

_CHANNELS = {
    "programmatic_dsp": 35,
    "global_boards": 20,
    "niche_boards": 15,
    "social_media": 12,
    "regional_boards": 8,
    "employer_branding": 5,
}
# Exactly what app.py builds for the wizard's string roles.
_WIZARD_ROLES = [{"title": "Registered Nurse", "count": 1, "tier": "Professional"}]
_DALLAS = [{"city": "Dallas", "state": "TX", "country": "United States"}]


def _plan(**kw):
    return be.calculate_budget_allocation(
        total_budget=250_000.0,
        roles=_WIZARD_ROLES,
        locations=_DALLAS,
        industry="healthcare_medical",
        channel_percentages=dict(_CHANNELS),
        synthesized_data={},
        knowledge_base=None,
        plan_currency="USD",
        budget_text="$250,000",
        **kw,
    )


class TestNoStatedGoal:
    def test_target_is_the_plans_projection_not_one_seat(self):
        res = _plan()
        hires = res["total_projected"]["hires"]
        rc = res["sufficiency"]["budget_reality_check"]
        assert res["metadata"]["total_openings"] == hires, res["metadata"]
        assert rc["target_hires"] == hires
        assert rc["realistic_hires"] == hires
        assert "$250,000/hire" not in rc["feasibility_message"]
        assert rc["feasibility_label"] != "WELL-FUNDED", rc

    def test_source_is_recorded(self):
        assert _plan()["metadata"]["target_hires_source"] == "projected_hires"


class TestStatedGoal:
    def test_goal_drives_budget_per_hire_and_shortfall(self):
        res = _plan(target_hires=60)
        suff = res["sufficiency"]
        hires = res["total_projected"]["hires"]
        assert res["metadata"]["total_openings"] == 60
        assert res["metadata"]["target_hires_source"] == "stated_goal"
        assert suff["budget_per_opening"] == pytest.approx(250_000 / 60, abs=0.01)
        assert suff["budget_reality_check"]["target_hires"] == 60
        assert suff["budget_reality_check"]["realistic_hires"] == hires
        assert any(
            f"Projected hires ({hires}) fall short of the 60 target" in w
            for w in suff["warnings"]
        ), suff["warnings"]

    def test_explicit_role_headcounts_still_count(self):
        res = be.calculate_budget_allocation(
            total_budget=250_000.0,
            roles=[{"title": "Registered Nurse", "count": 40, "tier": "Clinical"}],
            locations=_DALLAS,
            industry="healthcare_medical",
            channel_percentages=dict(_CHANNELS),
            knowledge_base=None,
        )
        assert res["metadata"]["total_openings"] == 40
        assert res["metadata"]["target_hires_source"] == "role_counts"


class TestAppPassesTheWizardGoal:
    @pytest.mark.parametrize(
        "hire_volume,expected",
        [("60 hires", 60), ("100-500 hires", 100), ("5,000+", 5000), ("Not specified", 0), (None, 0)],
    )
    def test_parse(self, hire_volume, expected):
        assert app._hire_goal_for_budget({"hire_volume": hire_volume}) == expected

    def test_every_call_site_forwards_target_hires(self):
        src = (PROJECT_ROOT / "app.py").read_text()
        idx, sites = 0, 0
        while True:
            call = src.find("budget_result = calculate_budget_allocation(", idx)
            if call == -1:
                break
            # the call's closing paren: first ")\n" after its budget_text= arg
            end = src.find(")\n", src.find("budget_text", call))
            block = src[call : end + 2]
            assert "target_hires=_hire_goal_for_budget(" in block, block[-400:]
            sites += 1
            idx = end + 2
        assert sites == 3
