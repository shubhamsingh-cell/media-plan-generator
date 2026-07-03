#!/usr/bin/env python3
"""Render a New Zealand sample deliverable (mirrors the Pratt & Whitney NZ bundle
that surfaced the currency/localization defects in BUNDLE_QC_FINDINGS_2026-07-03.json).

Single-location NZ plan so localization/currency bugs are exercised the same
way the client bundle exercised them: NZD budget, Aircraft Tradesperson role,
Aerospace & Defense industry, US-only niche boards must NOT appear.

Usage:  python3 scripts/render_sample_nz.py [output_dir]
"""

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

OUT_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else PROJECT_ROOT / "tmp_render_nz"
OUT_DIR.mkdir(parents=True, exist_ok=True)

ROLES = [{"title": "Aircraft Tradesperson", "count": 30, "tier": "senior"}]
LOCATIONS = [{"city": "Auckland", "state": "", "country": "New Zealand"}]
CHANNELS = {
    "Niche / Industry Boards": 31,
    "Programmatic DSP": 21,
    "Global Job Boards": 16,
    "Employer Branding": 13,
    "Regional Boards": 11,
    "APAC Regional": 4,
    "Social Media": 3,
    "EMEA Regional": 1,
}


def build_plan_data() -> dict:
    import budget_engine

    alloc = budget_engine.calculate_budget_allocation(
        total_budget=150_000,
        roles=ROLES,
        locations=LOCATIONS,
        industry="aerospace_defense",
        channel_percentages=CHANNELS,
        collar_type="blue",
        campaign_start_month=7,
    )
    return {
        "client_name": "Pratt & Whitney New Zealand",
        "requester_name": "Shubham Singh Chandel",
        "requester_email": "shubhamsingh@joveo.com",
        "industry": "aerospace_defense",
        "budget": "NZ$150,000",
        "budget_period": "campaign",
        "campaign_duration": "12 weeks",
        "campaign_start_month": 7,
        "hire_volume": "30 hires",
        "work_environment": "hybrid",
        "experience_level": "senior",
        "campaign_goals": ["cost_efficiency", "speed_to_hire"],
        "roles": [r["title"] for r in ROLES],
        "target_roles": ROLES,
        "locations": [f"{l['city']}, {l['country']}" for l in LOCATIONS],
        "country": "New Zealand",
        "competitors": ["Raytheon Technologies", "Boeing", "Airbus"],
        "_budget_allocation": alloc,
        # Minimal salary_intelligence so the exec-summary SITUATION card's
        # "Salary Range" row is actually exercised by this fixture (the real
        # app.py enrichment pipeline attaches this; without it, this fixture
        # never tested the card's Salary Range row at all, fixed vs dropped).
        "_synthesized": {
            "salary_intelligence": {
                "Aircraft Tradesperson": {
                    "median": 85000,
                    "min": 55000,
                    "max": 140000,
                }
            }
        },
    }


def main() -> int:
    data = build_plan_data()
    failures = 0

    t0 = time.time()
    try:
        from ppt_generator import generate_pptx

        pptx_bytes = generate_pptx(dict(data))
        (OUT_DIR / "sample_plan_nz.pptx").write_bytes(pptx_bytes)
        print(f"PPTX  ok  {len(pptx_bytes):,} bytes  ({time.time() - t0:.1f}s)")
    except Exception as exc:  # noqa: BLE001 - report-all harness
        failures += 1
        print(f"PPTX  FAIL: {exc!r}")

    t0 = time.time()
    try:
        from excel_v2 import generate_excel_v2

        xlsx_bytes = generate_excel_v2(dict(data))
        if isinstance(xlsx_bytes, tuple):
            xlsx_bytes = xlsx_bytes[0]
        (OUT_DIR / "sample_plan_nz.xlsx").write_bytes(xlsx_bytes)
        print(f"XLSX  ok  {len(xlsx_bytes):,} bytes  ({time.time() - t0:.1f}s)")
    except Exception as exc:  # noqa: BLE001
        failures += 1
        print(f"XLSX  FAIL: {exc!r}")

    print(f"Outputs in: {OUT_DIR}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
