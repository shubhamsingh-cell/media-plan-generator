"""Every surface shows applications / hires / CPA / CPH as low-high, with the
hires and cost-per-hire basis stated next to the number (P2 + P4).

Pre-fix the wizard tiles, Quick Plan, Nova's formatter, the workbook and the
deck each printed a lone point estimate (the optimistic end) with no statement
of what it assumed.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import sys
import types

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
for _name in ("anthropic", "openai", "supabase", "redis", "qdrant_client", "sentence_transformers", "posthog"):
    sys.modules.setdefault(_name, types.ModuleType(_name))
logging.disable(logging.CRITICAL)

DASH = "–"
SHAPES = [
    ("Warehouse Associate", "Dallas, TX", "logistics_supply_chain"),
    ("Registered Nurse", "Columbus, OH", "healthcare_medical"),
    ("Software Engineer", "Remote, US", "tech_engineering"),
    ("Customer Service Representative", "United States", "general_entry_level"),
]


def _read(rel):
    with open(os.path.join(PROJECT_ROOT, rel), encoding="utf-8") as fh:
        return fh.read()


@pytest.mark.parametrize("role,loc,ind", SHAPES)
def test_estimate_text_is_ranges_with_the_assumption_stated(role, loc, ind):
    import app

    est = app._compute_plan_estimate(
        {"budget": "$60,000", "industry": ind, "roles": [role], "locations": [loc], "client_name": "Acme"}
    )
    text = est["projection"]["text"]
    for key in ("applications", "hires", "cpa", "cph"):
        assert DASH in text[key], (key, text[key])
    assert "no client hire history" in text["hires_short"]
    assert "not media-only" in text["cph_short"]
    # the lone point the engine computed is the optimistic END of each range
    assert est["projection"]["hires"]["high"] >= est["est_hires"]
    # (the CPA range is the hull of the engine figure and the KB band, so when
    # the engine CPA sits INSIDE the band the high end can exceed the engine's
    # own application count; it is never below it)
    assert est["projection"]["applications"]["high"] >= est["est_applications"]


def test_the_headline_is_not_silently_the_optimistic_end():
    import app

    est = app._compute_plan_estimate(
        {"budget": "$60,000", "industry": "healthcare_medical", "roles": ["Registered Nurse"], "locations": ["Columbus, OH"]}
    )
    proj = est["projection"]
    assert proj["hires"]["low"] < proj["hires"]["high"]
    assert proj["cph"]["low"] < proj["cph"]["high"]
    assert proj["applications"]["low"] < proj["applications"]["high"]


def test_wizard_preview_paints_ranges_and_the_basis_line():
    js = _read("templates/partials/index/body_preview_js.html")
    html = _read("templates/partials/index/body_content.html")
    assert 'id="npBasis"' in html
    assert "proj.text.applications" in js and "proj.text.hires" in js and "proj.text.cpa" in js
    assert "no client hire history" in js or "hires_short" in js
    node = shutil.which("node")
    if node:
        for i, code in enumerate(re.findall(r"<script[^>]*>(.*?)</script>", js, re.S)):
            path = os.path.join(os.environ.get("TMPDIR", "/tmp"), f"proj_prev_{os.getpid()}_{i}.js")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(code)
            try:
                res = subprocess.run([node, "--check", path], capture_output=True, text=True)
            finally:
                os.unlink(path)
            assert res.returncode == 0, res.stderr


def test_quick_plan_page_shows_ranges():
    import quick_plan

    plan = quick_plan.generate_quick_plan("Registered Nurse", "Columbus, OH", 60000, "healthcare_medical")
    text = plan["total_projected"]["projection"]["text"]
    assert DASH in text["hires"] and DASH in text["cph"] and DASH in text["applications"]
    page = _read("templates/quick-plan.html")
    assert "pj.applications" in page and "pj.hires" in page and "pj.cph" in page
    assert 'id="qpTotalsNote"' in page


def test_nova_formatter_prints_ranges_and_the_basis():
    import nova

    nv = nova.Nova.__new__(nova.Nova)
    nv._data_cache = {}
    data = nv._query_budget_projection(
        {"budget": 60000, "roles": ["Registered Nurse"], "locations": ["Columbus, OH"], "industry": "general"}
    )
    out = nova._format_budget_response(data, 60000)
    assert "Estimated Hires: 5" + DASH + "11" in out
    assert "Cost per Application: $13.96" + DASH + "$58" in out
    assert "no client hire history" in out
    assert "Projected Hires:" not in out  # the lone point is gone
    assert "presentation_note" in data


def test_nova_roi_and_budget_agree_on_the_cph_concept():
    """P4: the CPH comparison says which concept it uses."""
    import plan_projection

    p = plan_projection.project_plan(
        {"budget": 60000, "roles": ["Registered Nurse"], "locations": ["Columbus, OH"]}
    )
    a = p["assumptions"]
    assert a["cph_concept"] == "total"
    assert "TOTAL cost per hire" in a["cph_basis"]
    # KB keys cited where present (healthcare carries a media-only figure)
    assert "cph.recruitment_marketing_only" in a["cph_basis"]
    assert a["client_hire_history"] is False
    # applicants_per_hire is cited when the KB has it (technology: 110)
    t = plan_projection.project_plan(
        {"budget": 60000, "roles": ["Software Engineer"], "locations": ["Remote, US"]}
    )
    assert "applicants_per_hire" in t["assumptions"]["applicants_per_hire_note"]
