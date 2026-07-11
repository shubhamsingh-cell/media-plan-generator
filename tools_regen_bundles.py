#!/usr/bin/env python3
"""Regenerate Nova MPG bundles (deck .pptx + workbook .xlsx, zipped) headless,
by calling the SAME internal functions app.py's ``/api/generate`` handler
calls -- without booting the HTTP server.

This mirrors (as closely as practical without running the full HTTP request
handler) the production pipeline segment that turns a validated brief into
the ``data`` dict passed to ``generate_pptx`` / ``generate_excel_v2``:

    1. app.classify_industry(...)            -> industry/legacy_key/talent_profile
    2. INDUSTRY_ALLOC_PROFILES[industry]      -> channel_percentages (SAME snake_case
                                                  keys production uses, e.g.
                                                  "programmatic_dsp", "global_boards")
    3. budget_engine.calculate_budget_allocation(...) -> data["_budget_allocation"]
    4. campaign_duration -> campaign_weeks (same regex ladder app.py uses)
    5. ppt_generator.generate_pptx(data) / excel_v2.generate_excel_v2(data)
    6. zip as "{descriptive_filename}.xlsx" + "{descriptive_filename}_Deck.pptx"
       (same naming app.py uses at the zip-build call site)

What this intentionally SKIPS (and why that's fine for bundle-QUALITY repro):
    - api_enrichment.enrich_data() / data_synthesizer full run -- these hit
      external paid APIs (BLS, Adzuna, FRED, Census, Google, etc.). On Render
      those keys ARE set; locally they are NOT. Production itself degrades
      gracefully when a given key is missing (each enrichment call is wrapped
      in try/except, see CLAUDE.md "Error isolation"), so omitting them here
      reproduces the SAME degraded-enrichment condition, not a different one.
    - competitive_intel / market_pulse / slotops live-benchmark injection --
      same reasoning (external APIs, optional, non-fatal on failure).
    - The Google Slides deck tier (deck_generator.py DeckGenerator, tier 1)
      -- requires GOOGLE_SLIDES_CREDENTIALS_B64, absent locally, and
      deck_generator.py's *own* fallback ladder collapses to tier 7
      (python-pptx == generate_pptx) when that key is missing. So calling
      generate_pptx directly reproduces exactly what production falls back
      to without that credential.

LLM-backed narrative text (executive summary narrative in excel_v2.py,
methodology/push-pull slide copy in ppt_generator.py) goes through
llm_router.py, which tries ANTHROPIC_API_KEY / DEEPSEEK_API_KEY /
OPENAI_API_KEY / GROQ_API_KEY (see llm_router.py MODEL_CATALOG). None of
these are set in this environment -- every call_llm() invocation raises
(auth/network error), is caught by a broad `except Exception` at the call
site, and the narrative section is silently omitted (not fatal, not a
placeholder). This script does not set fake keys; it deliberately runs
in that fallback mode SO THAT the fallback path itself is part of what
gets QC'd (per the task brief). Search this script's stdout for
"LLM narrative" / "router" warnings to see where that happened.

Usage:
    python3 tools_regen_bundles.py                 # both bundles -> ./out/
    python3 tools_regen_bundles.py --only manpower  # just one
    python3 tools_regen_bundles.py --out /tmp/x     # custom output dir

Re-runnable: safe to re-run after code changes in ppt_generator.py /
excel_v2.py / budget_engine.py / app.py -- just re-invoke.
"""

from __future__ import annotations

import argparse
import io
import re
import sys
import time
import zipfile
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))


# ─────────────────────────────────────────────────────────────────────────
# Brief definitions (reconstructed from the July-10 bundles per task brief)
# ─────────────────────────────────────────────────────────────────────────

MANPOWER_BRIEF: dict[str, Any] = {
    "client_name": "Manpower - Amerigas",
    "requester_name": "Shubham Singh Chandel",
    "requester_email": "shubhamsingh@joveo.com",
    "industry": "Logistics & Supply Chain",
    "budget": "$150,000",
    "campaign_duration": "6 months",
    "hire_volume": "100-500 hires",
    "work_environment": "onsite",
    "locations": [
        "Massachusetts",
        "Maine",
        "New Hampshire",
        "Rhode Island",
        "Connecticut",
        "Denver, CO",
    ],
    "roles": ["CDL A Driver"],
    "target_roles": [{"title": "CDL A Driver", "count": 300, "tier": "Hourly"}],
    "notes": "Blue Collar talent profile. On-Site work model. 6 markets: "
    "Massachusetts, Maine, New Hampshire, Rhode Island, Connecticut, Denver CO.",
}

ATRIA_ROLE_TITLES = [
    "Memory Care Associate",
    "Nurse",
    "Cook",
    "Driver",
    "Maintenance Technician",
    "Server/Waitstaff",
    "Shift/Charge Nurse",
    "Dishwasher",
    "Housekeeper",
    "Sales",
]

ATRIA_BRIEF: dict[str, Any] = {
    "client_name": "atria Senior living",  # deliberately lowercase, AS RECEIVED
    "requester_name": "Shubham Singh Chandel",
    "requester_email": "shubhamsingh@joveo.com",
    "industry": "Healthcare & Medical",
    "budget": "$300,000",
    "campaign_duration": "18 months",  # 1.5 years, as reconstructed
    "hire_volume": "500+ hires",
    "work_environment": "remote",  # AS-RECEIVED (on-site-only senior-living roles)
    "locations": ["New York, NY"],
    "roles": ATRIA_ROLE_TITLES,
    "target_roles": [
        {"title": t, "count": 50, "tier": "Hourly"} for t in ATRIA_ROLE_TITLES
    ],
    "notes": "10 roles for a senior living community: memory care, nurse, cook, "
    "driver, maintenance technician, server/waitstaff, shift/charge nurse, "
    "dishwasher, housekeeper, sales. Hire volume 500+.",
}


# ─────────────────────────────────────────────────────────────────────────
# Pipeline replication (mirrors app.py's /api/generate handler)
# ─────────────────────────────────────────────────────────────────────────


def _compute_campaign_weeks(campaign_duration: str) -> int:
    """Same regex ladder as app.py (~line 16151) that derives campaign_weeks
    from the free-text campaign_duration string, used for 90-day-forecast
    footnote gating and timeline phasing."""
    dur_lower = (campaign_duration or "").lower()
    if "2-5 year" in dur_lower or "long-term" in dur_lower or "long term" in dur_lower:
        return 156
    if "1-2 year" in dur_lower or "2 year" in dur_lower:
        return 80
    if (
        "6-12 month" in dur_lower
        or "9 month" in dur_lower
        or "12 month" in dur_lower
        or "1 year" in dur_lower
    ):
        return 48
    if (
        "3-6 month" in dur_lower
        or "4 month" in dur_lower
        or "5 month" in dur_lower
        or "6 month" in dur_lower
    ):
        return 24
    if (
        "1-3 month" in dur_lower
        or "1 month" in dur_lower
        or "2 month" in dur_lower
        or "3 month" in dur_lower
    ):
        return 12
    if "ongoing" in dur_lower:
        return 52
    wk_match = re.search(r"(\d+)\s*week", dur_lower)
    if wk_match:
        return int(wk_match.group(1))
    mo_match = re.search(r"(\d+)\s*month", dur_lower)
    if mo_match:
        # S91 FIX (matches app.py's real ladder, commit 32e5d2b): the old
        # `int(months) * 4` formula silently drifted for anything not a
        # multiple of 4 weeks/month (e.g. "18 months" -> 72 weeks, which
        # re-derives downstream as "17 months"). display_format was NOT
        # kept in sync with app.py's fix here previously -- this harness
        # was reproducing a duration bug app.py itself no longer has.
        import display_format as _df

        return _df.parse_duration_to_weeks(campaign_duration)
    return 12  # default, matches app.py


def build_plan_data(brief: dict[str, Any]) -> dict[str, Any]:
    """Build the ``data`` dict exactly as app.py's /api/generate handler
    would, right before calling generate_pptx / generate_excel_v2."""
    import app  # noqa: WPS433 -- intentional: reuse the SAME classify_industry
    import budget_engine
    from kb_loader import load_knowledge_base
    from ppt_generator import INDUSTRY_ALLOC_PROFILES

    data: dict[str, Any] = dict(brief)

    # ── Step 1: NAICS-based industry classification (app.py ~line 17122) ──
    industry_raw = data.get("industry") or ""
    company_name = data.get("client_name") or ""
    # app.py normalizes dict-of-dicts -> list-of-strings before this call
    # (its ~line 16144 "Normalize roles early" step); classify_industry()
    # itself does `" ".join(roles)` and blows up on dicts otherwise.
    _roles_raw_for_classify = data.get("target_roles") or data.get("roles") or []
    roles_list = [
        (r.get("title") or r.get("role") or str(r)) if isinstance(r, dict) else str(r)
        for r in _roles_raw_for_classify
    ]
    industry_profile = app.classify_industry(industry_raw, company_name, roles_list)
    data["industry"] = industry_profile.get("legacy_key", "general_entry_level")
    data["industry_label"] = industry_profile["sector"]
    data["talent_profile"] = industry_profile["talent_profile"]
    data["bls_sector"] = industry_profile["bls_sector"]
    data["naics_code"] = industry_profile.get("naics", "00")

    # ── Step 2: Channel percentages from INDUSTRY_ALLOC_PROFILES (app.py
    # ~line 17150) -- these dict KEYS are the exact snake_case identifiers
    # ("programmatic_dsp", "global_boards", "niche_boards", "social_media",
    # "regional_boards", "employer_branding") production uses; this is
    # the reproduction path for the snake_case-channel-name check. ──
    _DEFAULT_ALLOC_BA = {
        "programmatic_dsp": 35,
        "global_boards": 20,
        "niche_boards": 15,
        "social_media": 12,
        "regional_boards": 8,
        "employer_branding": 5,
        "apac_regional": 3,
        "emea_regional": 2,
    }
    channel_pcts = dict(
        INDUSTRY_ALLOC_PROFILES.get(data["industry"], _DEFAULT_ALLOC_BA)
    )
    # Both briefs are US-only -> strip APAC/EMEA and redistribute to the
    # top channel (same logic as app.py ~line 17252).
    intl_pct = channel_pcts.pop("apac_regional", 0) + channel_pcts.pop(
        "emea_regional", 0
    )
    if intl_pct > 0:
        top_ch = max(channel_pcts, key=lambda k: channel_pcts[k])
        channel_pcts[top_ch] = channel_pcts[top_ch] + intl_pct

    # ── Step 3: role/location dicts for the budget engine (app.py ~17340) ──
    roles_for_ba = []
    for r in data.get("target_roles") or []:
        if isinstance(r, dict):
            roles_for_ba.append(
                {
                    "title": r.get("title") or "",
                    "count": int(r.get("count", 1)),
                    "tier": r.get("tier", "Professional"),
                }
            )
        elif isinstance(r, str):
            roles_for_ba.append({"title": r, "count": 1, "tier": "Professional"})

    locs_for_ba = []
    for loc in data.get("locations") or []:
        parts = [p.strip() for p in str(loc).split(",")]
        locs_for_ba.append(
            {
                "city": parts[0] if parts else loc,
                "state": parts[1] if len(parts) > 1 else "",
                "country": parts[2] if len(parts) > 2 else "US",
            }
        )

    from shared_utils import parse_budget

    budget_val = parse_budget(str(data.get("budget") or ""))
    kb = load_knowledge_base()

    budget_result = budget_engine.calculate_budget_allocation(
        total_budget=budget_val,
        roles=roles_for_ba,
        locations=locs_for_ba,
        industry=data["industry"],
        channel_percentages=channel_pcts,
        synthesized_data={},  # no live enrichment locally -- see module docstring
        knowledge_base=kb,
        collar_type=data.get("_collar_type") or "",  # NEVER set in prod either
        campaign_start_month=int(data.get("campaign_start_month") or 0),
    )
    data["_budget_allocation"] = budget_result

    # ── Step 4: campaign_weeks (app.py ~line 16151) ──
    data["campaign_weeks"] = _compute_campaign_weeks(str(data.get("campaign_duration")))
    data.setdefault("campaign_start_month", 0)

    return data


def generate_bundle(
    brief: dict[str, Any], out_dir: Path, slug: str
) -> dict[str, Any]:
    """Build plan data, call generate_pptx + generate_excel_v2, zip them.

    Returns a dict of {"pptx_bytes", "xlsx_bytes", "zip_path", "elapsed_s",
    "errors": [...]}.
    """
    errors: list[str] = []
    t0 = time.time()
    data = build_plan_data(brief)
    t_build = time.time() - t0

    pptx_bytes = None
    xlsx_bytes = None

    t0 = time.time()
    try:
        from ppt_generator import generate_pptx

        pptx_bytes = generate_pptx(dict(data))
    except Exception as exc:  # noqa: BLE001 -- report-all harness
        errors.append(f"generate_pptx FAILED: {exc!r}")
    t_pptx = time.time() - t0

    t0 = time.time()
    try:
        from excel_v2 import generate_excel_v2
        from kb_loader import load_knowledge_base

        # app.py's production call site passes load_kb_fn=load_knowledge_base
        # (see app.py ~17724) -- without it, excel_v2's "Recruitment
        # Benchmarks" section on the Executive Summary sheet silently never
        # renders (its `if load_kb_fn:` guard is just skipped), which would
        # make any deck<->workbook benchmark cross-check meaningless here.
        xlsx_bytes = generate_excel_v2(dict(data), load_kb_fn=load_knowledge_base)
        if isinstance(xlsx_bytes, tuple):
            xlsx_bytes = xlsx_bytes[0]
    except Exception as exc:  # noqa: BLE001
        errors.append(f"generate_excel_v2 FAILED: {exc!r}")
    t_xlsx = time.time() - t0

    out_dir.mkdir(parents=True, exist_ok=True)
    descriptive = re.sub(r"[^a-zA-Z0-9_\-]", "_", data.get("client_name") or "Client")

    zip_path = None
    if pptx_bytes and xlsx_bytes:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(f"{descriptive}.xlsx", xlsx_bytes)
            zf.writestr(f"{descriptive}_Deck.pptx", pptx_bytes)
        zip_path = out_dir / f"{slug}_Media_Plan_Bundle.zip"
        zip_path.write_bytes(zip_buffer.getvalue())

    # Always also drop the raw files for easy inspection.
    if pptx_bytes:
        (out_dir / f"{slug}_Deck.pptx").write_bytes(pptx_bytes)
    if xlsx_bytes:
        (out_dir / f"{slug}.xlsx").write_bytes(xlsx_bytes)

    return {
        "slug": slug,
        "pptx_bytes": pptx_bytes,
        "xlsx_bytes": xlsx_bytes,
        "zip_path": zip_path,
        "errors": errors,
        "timings": {"build": t_build, "pptx": t_pptx, "xlsx": t_xlsx},
        "data": data,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=PROJECT_ROOT / "out",
        help="Output directory (default: ./out)",
    )
    parser.add_argument(
        "--only",
        choices=["manpower", "atria"],
        help="Only regenerate one bundle (default: both)",
    )
    args = parser.parse_args()

    briefs = {
        "manpower": MANPOWER_BRIEF,
        "atria": ATRIA_BRIEF,
    }
    if args.only:
        briefs = {args.only: briefs[args.only]}

    exit_code = 0
    for slug, brief in briefs.items():
        print(f"\n{'=' * 70}\nGenerating: {slug} ({brief['client_name']!r})\n{'=' * 70}")
        result = generate_bundle(brief, args.out, slug)
        t = result["timings"]
        print(
            f"  build={t['build']:.2f}s  pptx={t['pptx']:.2f}s  xlsx={t['xlsx']:.2f}s"
        )
        if result["pptx_bytes"]:
            print(f"  PPTX ok  {len(result['pptx_bytes']):,} bytes")
        if result["xlsx_bytes"]:
            print(f"  XLSX ok  {len(result['xlsx_bytes']):,} bytes")
        if result["zip_path"]:
            print(f"  ZIP  ok  {result['zip_path']}")
        for err in result["errors"]:
            print(f"  ERROR: {err}")
            exit_code = 1

    print(f"\nOutputs in: {args.out}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
