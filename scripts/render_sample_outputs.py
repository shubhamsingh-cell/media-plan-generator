#!/usr/bin/env python3
"""Render sample client deliverables (PPTX / Excel / scorecard HTML) locally.

Drives the REAL budget engine to build an authentic ``_budget_allocation``
payload, then runs the production generators so output quality can be
inspected (and visually diffed) without hitting the live server.

Usage:  python3 scripts/render_sample_outputs.py [output_dir]
"""

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

OUT_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else PROJECT_ROOT / "tmp_render"
OUT_DIR.mkdir(parents=True, exist_ok=True)

ROLES = [
    {"title": "Registered Nurse", "count": 40, "tier": "mid"},
    {"title": "ICU Nurse", "count": 15, "tier": "senior"},
    {"title": "Nurse Practitioner", "count": 10, "tier": "senior"},
    {"title": "Medical Assistant", "count": 25, "tier": "entry"},
]
LOCATIONS = [
    {"city": "Dallas", "state": "TX", "country": "United States"},
    {"city": "Houston", "state": "TX", "country": "United States"},
    {"city": "Phoenix", "state": "AZ", "country": "United States"},
]
# 11 channels on purpose -- exercises the pie-chart "Other" rollup path
CHANNELS = {
    "Indeed": 22,
    "Programmatic Job Boards": 18,
    "LinkedIn": 14,
    "Google Search Ads": 10,
    "Meta (Facebook/Instagram)": 8,
    "ZipRecruiter": 7,
    "Nurse.com": 6,
    "Health eCareers": 5,
    "Glassdoor": 4,
    "TikTok": 3,
    "Craigslist": 3,
}


def build_plan_data() -> dict:
    import budget_engine

    alloc = budget_engine.calculate_budget_allocation(
        total_budget=150_000,
        roles=ROLES,
        locations=LOCATIONS,
        industry="healthcare",
        channel_percentages=CHANNELS,
        collar_type="white",
        campaign_start_month=9,
    )
    return {
        "client_name": "Mercy Health Partners",
        "requester_name": "Shubham Singh Chandel",
        "requester_email": "shubhamsingh@joveo.com",
        "industry": "healthcare",
        "budget": "$150,000",
        "budget_period": "campaign",
        "campaign_duration": "3 months",
        "campaign_start_month": 9,
        "hire_volume": "90 hires",
        "work_environment": "onsite",
        "experience_level": "mixed",
        "campaign_goals": ["cost_efficiency", "quality_of_hire"],
        "roles": [r["title"] for r in ROLES],
        "target_roles": ROLES,
        "locations": [f"{l['city']}, {l['state']}" for l in LOCATIONS],
        "competitors": ["HCA Healthcare", "Baylor Scott & White", "Tenet Health"],
        "_budget_allocation": alloc,
    }


def main() -> int:
    data = build_plan_data()
    failures = 0

    t0 = time.time()
    try:
        from ppt_generator import generate_pptx

        pptx_bytes = generate_pptx(dict(data))
        (OUT_DIR / "sample_plan.pptx").write_bytes(pptx_bytes)
        print(f"PPTX  ok  {len(pptx_bytes):,} bytes  ({time.time() - t0:.1f}s)")
    except Exception as exc:  # noqa: BLE001 - report-all harness
        failures += 1
        print(f"PPTX  FAIL: {exc!r}")

    t0 = time.time()
    try:
        from excel_v2 import generate_excel_v2

        xlsx_bytes = generate_excel_v2(dict(data))
        if isinstance(xlsx_bytes, tuple):  # some versions return (bytes, meta)
            xlsx_bytes = xlsx_bytes[0]
        (OUT_DIR / "sample_plan.xlsx").write_bytes(xlsx_bytes)
        print(f"XLSX  ok  {len(xlsx_bytes):,} bytes  ({time.time() - t0:.1f}s)")
    except Exception as exc:  # noqa: BLE001
        failures += 1
        print(f"XLSX  FAIL: {exc!r}")

    t0 = time.time()
    try:
        from scorecard_generator import generate_scorecard_html

        html = generate_scorecard_html(dict(data), "sample123")
        (OUT_DIR / "sample_scorecard.html").write_text(html, encoding="utf-8")
        print(f"SCORE ok  {len(html):,} chars  ({time.time() - t0:.1f}s)")
    except Exception as exc:  # noqa: BLE001
        failures += 1
        print(f"SCORE FAIL: {exc!r}")

    print(f"Outputs in: {OUT_DIR}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
