"""Regression test for S91: ALL app.py call sites of
``calculate_budget_allocation`` (the async gen_data path, the legacy
synchronous ``data`` path, and -- since the plan-estimate fix -- the
``/api/estimate`` live-preview path) must resolve ``us_plan`` via
``plan_geo`` and ``vendor_availability`` via
``excel_v2.get_niche_vendor_availability`` before calling the budget
engine, and must forward ``vendor_availability=`` into the call.

The async site was fixed in S91 (commit 32e5d2b). The synchronous/legacy
site at the second ``calculate_budget_allocation(`` call was a parallel
code path that still built ``budget_result`` without ever computing
``us_plan``/``vendor_availability`` -- a channel with zero real vendor
coverage (e.g. niche boards outside a covered US industry) silently kept
its full budget share on that path instead of being floored per
``_apply_vendor_gate``. A third call site (``app._compute_plan_estimate``,
backing ``POST /api/estimate``) was added later to single-source the
wizard's live preview off the SAME engine call -- it mirrors the same
vendor-gating wiring so the preview never overstates a channel's yield
relative to what the final generated plan would actually produce.

app.py's request handler is not a standalone testable unit (~26k lines
deep inside a single function), so -- matching the pattern in
``test_app_duration_wiring.py`` -- this test verifies the WIRING via
source inspection of each call-site block, plus a direct unit check that
``excel_v2.get_niche_vendor_availability`` (the accessor every site
resolves through ``getattr``) actually floors a vendor-less channel when
fed through ``calculate_budget_allocation``.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402
import excel_v2  # noqa: E402
from budget_engine import calculate_budget_allocation  # noqa: E402


def _call_site_blocks() -> list[str]:
    """Slice app.py's source into the two windows around each
    ``calculate_budget_allocation(`` call (from 40 lines above the call
    through the call itself), so each site's immediately-preceding wiring
    can be inspected independently."""
    src = (PROJECT_ROOT / "app.py").read_text()
    blocks = []
    idx = 0
    while True:
        call_idx = src.find("budget_result = calculate_budget_allocation(", idx)
        if call_idx == -1:
            break
        window_start = src.rfind("\n", 0, call_idx - 3500) + 1
        call_end = src.index(")\n", call_idx) + 2
        blocks.append(src[window_start:call_end])
        idx = call_end
    return blocks


def test_exactly_three_calculate_budget_allocation_call_sites():
    blocks = _call_site_blocks()
    assert len(blocks) == 3, (
        f"Expected exactly 3 calculate_budget_allocation call sites "
        f"(async generate, legacy sync generate, /api/estimate preview), "
        f"found {len(blocks)} -- update this test's window size/assumptions "
        f"if a new call site was intentionally added"
    )


def test_all_call_sites_resolve_us_plan_via_plan_geo():
    for i, block in enumerate(_call_site_blocks()):
        assert "plan_geo.is_us_plan(" in block, (
            f"call site #{i + 1} does not resolve us_plan via "
            f"plan_geo.is_us_plan -- vendor gating cannot locale-check"
        )


def test_all_call_sites_resolve_vendor_availability_defensively():
    for i, block in enumerate(_call_site_blocks()):
        assert "get_niche_vendor_availability" in block, (
            f"call site #{i + 1} never looks up "
            f"excel_v2.get_niche_vendor_availability"
        )
        getattr_idx = block.find("getattr(")
        assert getattr_idx != -1 and "excel_v2" in block[getattr_idx : getattr_idx + 80], (
            f"call site #{i + 1} must resolve get_niche_vendor_availability "
            f"defensively via getattr(excel_v2, ...) (agent B's accessor "
            f"lands in parallel -- a hard attribute access would crash "
            f"budget allocation if it's missing)"
        )


def test_all_call_sites_forward_vendor_availability_kwarg():
    for i, block in enumerate(_call_site_blocks()):
        assert "vendor_availability=vendor_availability" in block, (
            f"call site #{i + 1} computes vendor_availability but never "
            f"passes it into calculate_budget_allocation -- the gate is a "
            f"no-op on this path"
        )


def test_niche_vendor_accessor_resolves_from_app_excel_v2():
    fn = getattr(app.excel_v2, "get_niche_vendor_availability", None)
    assert fn is not None, (
        "app.excel_v2.get_niche_vendor_availability is not resolvable -- "
        "the defensive getattr in both call sites would silently leave "
        "vendor_availability at None (no gating) in production"
    )
    assert fn is excel_v2.get_niche_vendor_availability


def test_channel_with_no_vendors_is_floored_through_calculate_budget_allocation():
    """Unit-level proof (2b): driving calculate_budget_allocation with a
    vendor_availability map that marks a channel unavailable floors that
    channel's share, exactly as app.py's wiring would produce for an
    industry/locale with zero covered niche boards."""
    roles = [{"title": "Registered Nurse", "count": 10, "tier": "Professional"}]
    locations = [{"city": "Austin", "state": "TX", "country": "US"}]
    channel_pcts = {
        "job_boards": 30,
        "niche_boards": 25,
        "social_media": 20,
        "programmatic": 15,
        "employer_branding": 10,
    }

    gated = calculate_budget_allocation(
        total_budget=50000,
        roles=roles,
        locations=locations,
        industry="Healthcare",
        channel_percentages=dict(channel_pcts),
        vendor_availability={"niche_boards": False},
    )
    ungated = calculate_budget_allocation(
        total_budget=50000,
        roles=roles,
        locations=locations,
        industry="Healthcare",
        channel_percentages=dict(channel_pcts),
        vendor_availability=None,
    )

    gated_niche = gated["channel_allocations"].get("niche_boards", {})
    ungated_niche = ungated["channel_allocations"].get("niche_boards", {})
    assert gated_niche, "niche_boards channel dropped entirely from allocation"
    assert gated_niche.get("percentage", 100) <= 3.01, (
        f"vendor-gated niche_boards channel should be floored to <=3%, "
        f"got {gated_niche.get('percentage')}"
    )
    assert gated_niche.get("percentage", 0) < ungated_niche.get("percentage", 0), (
        "vendor gate had no effect relative to the ungated allocation"
    )


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
