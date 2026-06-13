#!/usr/bin/env python3
"""parity_audit.py -- run the S89 #9 Supabase dual-read parity audit (READ-ONLY).

Reads each domain from BOTH Supabase and its JSON fallback and prints a
divergence report. Needs SUPABASE_URL / SUPABASE_ANON_KEY in the env for the
Supabase half (otherwise domains report json_only / no_data). Does NOT change
what the app serves.

Usage::

    python3 scripts/parity_audit.py                       # all domains, summary
    python3 scripts/parity_audit.py --json                # full JSON report
    python3 scripts/parity_audit.py channel_benchmarks    # named domains only
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from supabase_parity import run_parity_audit  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="S89 #9 Supabase dual-read parity audit")
    ap.add_argument("domains", nargs="*", help="domains to audit (default: all)")
    ap.add_argument("--json", action="store_true", help="emit the full JSON report")
    args = ap.parse_args()

    report = run_parity_audit(args.domains or None)

    if args.json:
        print(json.dumps(report, indent=2, default=str))
        return 0

    print(
        f"Supabase enabled: {report['supabase_enabled']}  |  "
        f"cutover_ready: {report['cutover_ready']}"
    )
    print(f"Verdicts: {report['verdict_counts']}\n")
    for d in report["domains"]:
        print(
            f"  {d['domain']:<20} {d.get('verdict', ''):<15} "
            f"sup={d.get('supabase_count', 0):<5} json={d.get('json_count', 0):<5} "
            f"matched={d.get('matched', 0):<5} mismatch={d.get('value_mismatches', 0)}"
        )
    if report["supabase_only_domains"]:
        print(
            "\n[!] Supabase-only (no JSON fallback -- single point of failure): "
            + ", ".join(report["supabase_only_domains"])
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
