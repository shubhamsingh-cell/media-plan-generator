"""Commit-time invariants for recruitment_industry_knowledge.json's string
CPC ranges (benchmarks.cost_per_click.by_platform).

This file is load-bearing in two places the comprehensive KB is not:

  1. budget_engine._extract_cpc_from_kb parses these strings as fallback #4
     of the /api/estimate CPC cascade (synthesized -> channel_benchmarks_live
     -> trend_engine -> THIS FILE -> static). At runtime fallback #4 only
     fires in a degraded process where trend_engine failed to import --
     trend_engine.get_benchmark carries its own ultimate fallback and never
     returns None/<=0 for a platform-mapped category -- but a format typo in
     a refreshed string would silently break that degraded mode.
  2. nova._query_knowledge_base serves the strings verbatim in
     platform-comparison chat answers.

The indeed band here, the seed band in channel_benchmarks_seed.json, and the
comprehensive KB's cpc_by_platform entry (pinned against the seed by
tests/test_cpc_monitor.py) all derive from the same cited July-2026 research
(seed refresh 336480d, comprehensive-KB reconcile ea06bb0). If a future
session refreshes one file without the others, these tests fail -- refresh
all from the same cited research, or consciously re-baseline.

Runs under pytest, or standalone: ``python3 tests/test_industry_kb_cpc_strings.py``.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import budget_engine  # noqa: E402

_DOLLAR_RANGE = re.compile(r"^\$(\d+(?:\.\d+)?)-\$(\d+(?:\.\d+)?)$")


def _load_json(rel: str) -> dict:
    with open(PROJECT_ROOT / rel, encoding="utf-8") as f:
        return json.load(f)


def _parse_range(s: str) -> tuple:
    m = _DOLLAR_RANGE.match(s)
    assert m, f"CPC range string not in $X.XX-$Y.YY format: {s!r}"
    return float(m.group(1)), float(m.group(2))


def _seed_band(seed: dict, channel: str) -> tuple:
    for entry in seed["data"]:
        if entry.get("channel") == channel:
            rng = (entry.get("metadata") or {}).get("cpc_range") or {}
            return rng.get("min"), rng.get("max")
    raise AssertionError(f"channel {channel!r} not in channel_benchmarks_seed.json")


def test_indeed_string_band_matches_cited_seed_band():
    kb = _load_json("data/recruitment_industry_knowledge.json")
    seed = _load_json("data/channel_benchmarks_seed.json")
    kb_band = _parse_range(
        kb["benchmarks"]["cost_per_click"]["by_platform"]["indeed"]["average_cpc_range"]
    )
    assert kb_band == _seed_band(seed, "indeed"), (
        "indeed average_cpc_range diverged from channel_benchmarks_seed.json -- "
        "refresh both from the same cited research or re-baseline consciously"
    )


def test_linkedin_job_ads_string_band_matches_cited_seed_band():
    kb = _load_json("data/recruitment_industry_knowledge.json")
    seed = _load_json("data/channel_benchmarks_seed.json")
    kb_band = _parse_range(
        kb["benchmarks"]["cost_per_click"]["by_platform"]["linkedin"][
            "job_ad_cpc_range"
        ]
    )
    assert kb_band == _seed_band(seed, "linkedin"), (
        "linkedin job_ad_cpc_range diverged from channel_benchmarks_seed.json -- "
        "refresh both from the same cited research or re-baseline consciously"
    )


def test_kb_fallback_parses_every_platform_mapped_category():
    """Degraded-mode guard: _extract_cpc_from_kb must yield a positive CPC
    for every category its platform_map covers, and None for the categories
    it deliberately leaves CPC-less. Fails if a string refresh breaks the
    $X.XX-$Y.YY / $X.XX parse format budget_engine expects."""
    kb = _load_json("data/recruitment_industry_knowledge.json")
    parseable = [
        "search",
        "display",
        "social",
        "programmatic",
        "job_board",
        "niche_board",
        "regional",
        "employer_branding",
    ]
    for category in parseable:
        cpc = budget_engine._extract_cpc_from_kb(category, kb)
        assert cpc is not None and cpc > 0, (
            f"KB fallback #4 no longer resolves category {category!r} -- "
            "a cost_per_click.by_platform string is missing or unparseable"
        )
    for category in ["email", "career_site", "referral", "events", "staffing"]:
        assert budget_engine._extract_cpc_from_kb(category, kb) is None


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
