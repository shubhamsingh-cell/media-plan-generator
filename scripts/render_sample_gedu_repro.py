#!/usr/bin/env python3
"""Reproduce the Gedu_Media_Plan_Bundle.zip scenario (reviewed 2026-07-03,
generated ~13:18 that day -- before today's fix session) against the CURRENT
code, to determine which of its visible defects are already fixed vs still
live: tiny GBP budget (GBP3,000), Education industry, 1 UK location, 1 role
("Lecturer"), 0 projected hires (a near-zero-budget edge case our other test
fixtures never exercised).

Usage:  python3 scripts/render_sample_gedu_repro.py [output_dir]
"""

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

OUT_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else PROJECT_ROOT / "tmp_render_gedu"
OUT_DIR.mkdir(parents=True, exist_ok=True)

ROLES = [{"title": "Lecturer", "count": 1, "tier": "professional"}]
LOCATIONS = [{"city": "London", "state": "", "country": "United Kingdom"}]
CHANNELS = {
    "Niche / Industry Boards": 28,
    "Programmatic DSP": 20,
    "Global Job Boards": 18,
    "Social Media": 12,
    "Regional Boards": 10,
    "Employer Branding": 7,
    "APAC Regional": 3,
    "EMEA Regional": 2,
}


def build_plan_data() -> dict:
    import budget_engine

    alloc = budget_engine.calculate_budget_allocation(
        total_budget=3_000,
        roles=ROLES,
        locations=LOCATIONS,
        industry="education",
        channel_percentages=CHANNELS,
        collar_type="white",
        campaign_start_month=7,
    )
    return {
        "client_name": "Gedu",
        "requester_name": "Shubham Singh Chandel",
        "requester_email": "shubhamsingh@joveo.com",
        "industry": "education",
        "budget": "GBP3,000",
        "budget_period": "campaign",
        "campaign_duration": "1-3 months",
        "campaign_start_month": 7,
        "hire_volume": "TBD",
        "work_environment": "onsite",
        "experience_level": "professional",
        "campaign_goals": ["cost_efficiency"],
        "roles": [r["title"] for r in ROLES],
        "target_roles": ROLES,
        "locations": [f"{l['city']}, {l['country']}" for l in LOCATIONS],
        "country": "United Kingdom",
        "_budget_allocation": alloc,
    }


def main() -> int:
    data = build_plan_data()
    failures = 0

    t0 = time.time()
    try:
        from ppt_generator import generate_pptx

        pptx_bytes = generate_pptx(dict(data))
        (OUT_DIR / "gedu_repro.pptx").write_bytes(pptx_bytes)
        print(f"PPTX  ok  {len(pptx_bytes):,} bytes  ({time.time() - t0:.1f}s)")
    except Exception as exc:  # noqa: BLE001
        failures += 1
        print(f"PPTX  FAIL: {exc!r}")

    t0 = time.time()
    try:
        from excel_v2 import generate_excel_v2

        xlsx_bytes = generate_excel_v2(dict(data))
        if isinstance(xlsx_bytes, tuple):
            xlsx_bytes = xlsx_bytes[0]
        (OUT_DIR / "gedu_repro.xlsx").write_bytes(xlsx_bytes)
        print(f"XLSX  ok  {len(xlsx_bytes):,} bytes  ({time.time() - t0:.1f}s)")
    except Exception as exc:  # noqa: BLE001
        failures += 1
        print(f"XLSX  FAIL: {exc!r}")

    print(f"Outputs in: {OUT_DIR}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
