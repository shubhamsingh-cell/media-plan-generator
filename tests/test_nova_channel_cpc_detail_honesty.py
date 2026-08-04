"""Nova "Channel-level CPC detail" table honesty (2026-08-04).

nova.py's _fast_path_benchmark_lookup rendered a 9-vertical x 5-platform
table (42 CPC-shaped cells classified CPC_UNCITED_SURFACE) with zero
citation -- every figure was a hand-typed literal with no traceable source.

Restructured into a module-level table (nova._CHANNEL_CPC_DETAIL) where
every row carries a 4th "source" field:
  - "KB: <platform_key>.<field>" for platforms with a cited
    data/recruitment_industry_knowledge.json benchmarks.cost_per_click
    .by_platform entry -- the row's cpc_text now IS that entry's figure.
  - "internal estimate -- not independently benchmarked" for niche/
    specialist boards with no KB entry -- the PRE-EXISTING figure is kept
    (never replaced with an invented one), just disclosed.
  - "n/a -- not a per-click price" for the 3 rows that were never CPC
    figures to begin with (IncredibleHealth pay-per-hire, DAT Solutions
    carrier-matching, Craigslist flat-fee posting).

Runs under pytest, or standalone:
``python3 tests/test_nova_channel_cpc_detail_honesty.py``.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import nova  # noqa: E402


def _real_kb() -> dict:
    kb_path = PROJECT_ROOT / "data" / "recruitment_industry_knowledge.json"
    with open(kb_path, encoding="utf-8") as f:
        return json.load(f)


def _deep_kb() -> dict:
    kb_path = PROJECT_ROOT / "data" / "recruitment_benchmarks_deep.json"
    with open(kb_path, encoding="utf-8") as f:
        return json.load(f)


# Prefix for the (2026-08-04, S5 precision fix) vertical-specific source
# kind: "KB: recruitment_benchmarks_deep.industry_benchmarks.<industry>.cpc
# .<indeed_cpc|linkedin_cpc>". Distinguishes it from the flat "KB:
# <platform_key>.<field>" kind, which traces against
# data/recruitment_industry_knowledge.json's by_platform map instead.
_DEEP_KB_SOURCE_PREFIX = "recruitment_benchmarks_deep."


# Same token shape as tests/test_nova_platform_comparison_honesty.py's
# _DOLLAR_TOKEN -- captures magnitude suffixes into the token and requires a
# non-digit follower so truncated figures can't false-pass as a prefix of a
# longer cited one.
_DOLLAR_TOKEN = re.compile(
    r"\$\d[\d,]*(?:\.\d+)?(?:-\$\d[\d,]*(?:\.\d+)?)?\+?[kKmMbB]?"
)

_NON_CPC_DISCLOSURE = "n/a -- not a per-click price"
_ESTIMATE_DISCLOSURE = "internal estimate -- not independently benchmarked"

# The 3 rows that were never CPC figures (kept verbatim, disclosed as such).
_NON_CPC_PLATFORMS = {"IncredibleHealth", "DAT Solutions", "Craigslist (gigs)"}


def test_every_row_has_four_fields_and_a_recognized_source():
    """Structural check: every row is a 4-tuple, and every source is one of
    the three recognized kinds -- nothing silently uncited."""
    for vertical, rows in nova._CHANNEL_CPC_DETAIL.items():
        assert rows, f"{vertical} has no rows"
        for row in rows:
            assert len(row) == 4, f"{vertical} row {row!r} is not a 4-tuple"
            platform, cpc, note, source = row
            is_kb = source.startswith("KB: ")
            is_estimate = source == _ESTIMATE_DISCLOSURE
            is_non_cpc = source == _NON_CPC_DISCLOSURE
            assert is_kb or is_estimate or is_non_cpc, (
                f"{vertical}/{platform}: unrecognized source {source!r}"
            )
            if platform in _NON_CPC_PLATFORMS:
                assert is_non_cpc, (
                    f"{vertical}/{platform} is a non-CPC pricing model and "
                    f"must carry the non-CPC disclosure, got {source!r}"
                )


def test_kb_sourced_rows_trace_to_the_cited_kb_entry():
    """Every dollar figure in a "KB: ..."-sourced row must literally appear
    in the KB entry the source string names (same mechanism as
    test_every_fallback_dollar_figure_traces_to_cited_kb_entry in
    test_nova_platform_comparison_honesty.py). Two source kinds are
    recognized:
      - flat "KB: <platform_key>.<field>" -- traces against
        data/recruitment_industry_knowledge.json's benchmarks.cost_per_click
        .by_platform map.
      - vertical-specific "KB: recruitment_benchmarks_deep.industry_benchmarks
        .<industry>.cpc.<field>" -- traces against
        data/recruitment_benchmarks_deep.json by walking the dotted path
        (2026-08-04, S5 precision fix: some industries have a more precise
        indeed_cpc/linkedin_cpc field than the flat platform-level figure)."""
    kb = _real_kb()
    bp = kb["benchmarks"]["cost_per_click"]["by_platform"]
    deep_kb = _deep_kb()
    checked = 0

    for vertical, rows in nova._CHANNEL_CPC_DETAIL.items():
        for platform, cpc, note, source in rows:
            if not source.startswith("KB: "):
                continue
            ref = source[len("KB: ") :]

            if ref.startswith(_DEEP_KB_SOURCE_PREFIX):
                path_parts = ref[len(_DEEP_KB_SOURCE_PREFIX) :].split(".")
                node = deep_kb
                for part in path_parts:
                    assert isinstance(node, dict) and part in node, (
                        f"{vertical}/{platform}: source path {ref!r} -- "
                        f"{part!r} not found while walking "
                        f"data/recruitment_benchmarks_deep.json "
                        f"(path so far: {path_parts})"
                    )
                    node = node[part]
                cited_value = str(node)
                field_desc = ref
            else:
                platform_key, field = ref.split(".", 1)
                assert platform_key in bp, (
                    f"{vertical}/{platform}: source names KB platform "
                    f"{platform_key!r} which doesn't exist"
                )
                entry = bp[platform_key]
                assert field in entry, (
                    f"{vertical}/{platform}: source names KB field {field!r} "
                    f"not present in by_platform[{platform_key!r}]"
                )
                cited_value = str(entry[field])
                field_desc = f"{platform_key}.{field}"

            tokens = _DOLLAR_TOKEN.findall(cpc)
            assert tokens, f"{vertical}/{platform}: KB-sourced row has no dollar figure to check: {cpc!r}"
            for token in tokens:
                assert re.search(re.escape(token) + r"(?!\d)", cited_value), (
                    f"{vertical}/{platform}: figure {token!r} (from {cpc!r}) "
                    f"not found in cited KB value {cited_value!r} "
                    f"({field_desc})"
                )
            checked += 1

    # Non-vacuousness: the table does have KB-sourced rows to check.
    assert checked >= 10, f"expected >=10 KB-sourced rows, found {checked}"


def test_estimate_rows_keep_the_prior_uncited_value_not_a_new_one():
    """Non-vacuousness for the disclosure fix: rows without KB backing must
    KEEP their pre-existing figure (never invent a new one) -- spot-check a
    representative sample against the numbers that shipped before this fix."""
    prior_values = {
        ("healthcare", "Health eCareers"): "$2.00 – $5.50",
        ("nursing", "Nurse.com"): "$3.50 – $7.50",
        ("physician", "Doximity"): "$8.00 – $18.00",
        ("technology", "Dice"): "$3.50 – $8.00",
        ("retail", "Snagajob"): "$0.40 – $1.10",
        ("logistics", "CDLjobs.com"): "$1.20 – $3.00",
        ("finance", "Wall Street Oasis"): "$2.00 – $5.00",
        ("skilled_trades", "Trade-specific boards"): "$0.80 – $2.30",
        ("hospitality", "Culinary Agents"): "$0.80 – $2.00",
    }
    by_key = {
        (vertical, row[0]): row
        for vertical, rows in nova._CHANNEL_CPC_DETAIL.items()
        for row in rows
    }
    for key, expected_cpc in prior_values.items():
        row = by_key[key]
        assert row[1] == expected_cpc, (
            f"{key}: value changed from {expected_cpc!r} to {row[1]!r} -- "
            "estimate rows must keep the prior figure, not a new one"
        )
        assert row[3] == _ESTIMATE_DISCLOSURE


def test_retired_uncited_platform_level_bands_are_gone():
    """Drift guard: the platform-level bands that shipped uncited (distinct
    per vertical, never matching the KB) must never reappear for a KB-backed
    platform. Before the fix, Indeed alone had 8 different uncited bands
    across verticals ($1.25-$3.50 healthcare, $2.50-$6.50 nursing, ... );
    after the fix every Indeed row must be either the flat cited
    $0.97-$2.71 or, where the KB has a more precise vertical-specific
    figure (2026-08-04, S5 precision fix), that exact figure -- never one
    of the old uncited literals."""
    retired_indeed_bands = {
        "$1.25 – $3.50",
        "$2.50 – $6.50",
        "$1.80 – $5.00",
        "$0.50 – $1.25",
        "$1.00 – $2.50",
        "$1.50 – $4.00",
        "$0.60 – $1.80",
        "$0.40 – $1.10",
    }
    retired_linkedin_bands = {
        "$4.00 – $9.00",
        "$5.00 – $11.00",
        "$6.00 – $15.00",
        "$3.00 – $7.00",
        "$2.50 – $6.50",
    }
    # (vertical, platform) rows that now correctly render a KB
    # vertical-specific figure (data/recruitment_benchmarks_deep.json
    # industry_benchmarks.<industry>.cpc.{indeed_cpc,linkedin_cpc}) instead
    # of the flat platform-level one -- exempt from the flat-equality
    # assertion below, but pinned here to their exact KB-cited value so
    # this exemption can't silently mask a return to an uncited literal.
    vertical_specific_overrides = {
        ("technology", "Indeed"): "$0.50 – $2.00",
        ("retail", "Indeed"): "$0.20 – $0.75",
        ("finance", "Indeed"): "$0.50 – $1.50",
        ("finance", "LinkedIn"): "$4.50 – $7.00",
    }
    for vertical, rows in nova._CHANNEL_CPC_DETAIL.items():
        for platform, cpc, note, source in rows:
            override = vertical_specific_overrides.get((vertical, platform))
            if platform in ("Indeed", "Indeed (sponsored)"):
                assert cpc not in retired_indeed_bands, (
                    f"{vertical}: retired uncited Indeed band {cpc!r} still present"
                )
                assert cpc == (override if override else "$0.97 – $2.71"), (
                    f"{vertical}/Indeed: expected "
                    f"{(override or '$0.97 – $2.71')!r}, got {cpc!r}"
                )
            if platform in ("LinkedIn", "LinkedIn Jobs"):
                assert cpc not in retired_linkedin_bands, (
                    f"{vertical}: retired uncited LinkedIn band {cpc!r} still present"
                )
                assert cpc == (override if override else "$1.50 – $4.50"), (
                    f"{vertical}/LinkedIn: expected "
                    f"{(override or '$1.50 – $4.50')!r}, got {cpc!r}"
                )


def test_rendered_table_shows_the_source_column():
    """End-to-end: the actual chat response for a CPC benchmark question
    renders the Source column and at least one KB citation, not just the
    internal data structure."""
    n = nova.Nova.__new__(nova.Nova)
    msg = "what is the average cpc for nursing roles"
    result = n._fast_path_benchmark_lookup(msg, msg.lower())
    assert result is not None, "fast-path benchmark lookup did not match"
    resp = result["response"]
    assert "| Platform | Typical CPC | Notes | Source |" in resp
    assert "KB: indeed.average_cpc_range" in resp
    assert "KB: linkedin.job_ad_cpc_range" in resp
    assert _ESTIMATE_DISCLOSURE in resp
    assert _NON_CPC_DISCLOSURE in resp
    # Retired uncited nursing bands must be gone from the rendered text.
    assert "$5.00 – $11.00" not in resp
    assert "$2.50 – $6.50" not in resp


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
