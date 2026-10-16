"""The hub's Prediction Engine card, the wizard and /api/estimate read ONE
projection (plan_projection.project_plan): the card's headline figures are the
high end of its ranges, its basis line names the low end, and the range claims
carry the identical text. (Root cause of the post-rebase red: the merged
_compute_plan_estimate referenced `total_projected`, which the project_plan
routing had removed -- every estimate 500ed.)"""

from __future__ import annotations

import logging
import os
import sys
import types

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
for _n in ("anthropic", "openai", "supabase", "redis", "qdrant_client", "sentence_transformers", "posthog"):
    sys.modules.setdefault(_n, types.ModuleType(_n))
logging.disable(logging.CRITICAL)

import app  # noqa: E402
import plan_projection  # noqa: E402
import proof_claims  # noqa: E402


def _fresh():
    proof_claims.refresh()
    return proof_claims.claims()


def test_estimate_still_carries_the_hub_fields_and_the_projection():
    est = app._compute_plan_estimate(dict(proof_claims.HUB_PE_DEFAULT_BRIEF))
    for k in ("est_hires_midpoint", "industry_midpoint_cph", "cph_floor_applied", "cph_floor", "projection"):
        assert k in est, k
    assert est["est_hires_midpoint"] == est["projection"]["hires"]["low"]


def test_hub_claims_equal_the_estimates_projection():
    c = _fresh()
    est = app._compute_plan_estimate(dict(proof_claims.HUB_PE_DEFAULT_BRIEF))
    proj = est["projection"]
    assert int(c["pe_hires"].text.replace(",", "")) == proj["hires"]["high"]
    assert c["pe_hires_range"].text == proj["text"]["hires"]
    assert c["pe_cph_range"].text == proj["text"]["cph"]
    assert c["pe_apps_range"].text == proj["text"]["applications"]
    assert c["pe_cpa_range"].text == proj["text"]["cpa"]
    # the basis line names the low end of the same hires range
    assert f"{proj['hires']['low']}" in c["pe_basis"].text


def test_hub_default_brief_matches_a_direct_project_plan_call():
    brief = proof_claims.HUB_PE_DEFAULT_BRIEF
    direct = plan_projection.project_plan(
        {"budget": float(str(brief["budget"]).replace("$", "").replace(",", "")),
         "roles": brief["roles"], "locations": brief["locations"], "industry": brief.get("industry", ""),
         "client_name": brief.get("client_name", "")}
    )
    est = app._compute_plan_estimate(dict(brief))["projection"]
    for m in ("applications", "hires", "cpa", "cph"):
        assert (direct[m]["low"], direct[m]["high"]) == (est[m]["low"], est[m]["high"]), m
