"""Nova rule-based platform-comparison honesty (2026-07-16).

Three user-facing honesty defects in Nova._chat_rule_based's platform-
comparison branch, confirmed empirically during the July-2026 CPC string
refresh review:

1. "X vs Google Ads" never matched the KB's cited ``google_search_ads``
   entry -- "google_ads" is a substring of neither "google_search_ads"
   nor "google_display_ads" -- so every such comparison served an uncited
   hardcoded "$1.00-$4.00" band contradicting the KB's WordStream/LOCALiQ
   figures. Fixed with an explicit alias to ``google_search_ads``.
2. Glassdoor has no KB by_platform entry, so a hardcoded "$0.50-$2.00"
   band was always served -- but channel_benchmarks_seed.json's glassdoor
   entry deliberately retires standalone Glassdoor CPC (job ads run on
   Indeed's CPC engine since the Sept-2025 Indeed/Glassdoor
   consolidation). The summary is now the honest planned-via-Indeed line
   with no fabricated band.
3. The ZipRecruiter fallback summary (fires only on whole-KB load
   failure) said "Pay-per-click ... $0.50-$2.00" while the KB entry it
   stands in for says Subscription-based with an estimated CPC
   equivalent of $0.80-$1.00.

Uses ``Nova.__new__(Nova)`` to get a bound-method instance without the
heavy ``__init__`` (KB load), matching the pattern in
tests/test_truncation_precedence_nova.py.

Runs under pytest, or standalone:
``python3 tests/test_nova_platform_comparison_honesty.py``.
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


def _nova_with_kb(kb: dict) -> "nova.Nova":
    n = nova.Nova.__new__(nova.Nova)
    n._data_cache = {"knowledge_base": kb}
    return n


def test_google_ads_comparison_serves_cited_kb_entry():
    """ "Indeed vs Google Ads" must serve the KB's cited google_search_ads
    figures, not the retired uncited hardcoded band."""
    n = _nova_with_kb(_real_kb())
    resp = n._chat_rule_based("indeed vs google ads")["response"]
    # KB google_search_ads (WordStream/LOCALiQ-cited entry)
    assert "$5.26" in resp, "average_cpc_all_industries not served"
    assert "$3.00-$5.00" in resp, "employment_services_cpc_estimated not served"
    # The retired uncited fallback band must be gone
    assert "$1.00-$4.00" not in resp


def test_google_alias_also_reaches_kb_entry():
    """The bare "google" alias canonicalizes to Google Ads and must reach
    the same cited KB entry."""
    n = _nova_with_kb(_real_kb())
    resp = n._chat_rule_based("linkedin vs google for recruitment")["response"]
    assert "$5.26" in resp
    assert "$1.00-$4.00" not in resp


def test_glassdoor_comparison_is_honest_about_indeed_integration():
    """Glassdoor has no KB entry and the seed deliberately retires its
    standalone CPC -- the summary must say planned-via-Indeed, not serve
    the retired "$0.50-$2.00" band."""
    n = _nova_with_kb(_real_kb())
    resp = n._chat_rule_based("linkedin vs glassdoor")["response"]
    assert "$0.50-$2.00" not in resp, "retired uncited Glassdoor band served"
    assert "no standalone" in resp.lower()
    assert "Indeed's CPC engine" in resp


def test_ziprecruiter_fallback_matches_kb_model_and_band():
    """On whole-KB load failure the ZipRecruiter summary must agree with
    the KB entry it stands in for: Subscription-based, est. CPC
    equivalent $0.80-$1.00 -- not "Pay-per-click ... $0.50-$2.00"."""
    n = _nova_with_kb({})  # simulates KB load failure
    resp = n._chat_rule_based("ziprecruiter vs linkedin")["response"]
    assert "Subscription-based" in resp
    assert "$0.80-$1.00" in resp
    assert "$0.50-$2.00" not in resp
    assert "Pay-per-click" not in resp


def test_fallback_summaries_pinned_to_kb_strings():
    """Drift guard: the numeric strings in the hardcoded fallback
    summaries must literally appear in the KB entries they stand in for.
    If a future KB refresh changes a band without updating the fallback
    (or vice versa), this fails."""
    kb = _real_kb()
    bp = kb["benchmarks"]["cost_per_click"]["by_platform"]
    n = _nova_with_kb({})  # force every platform onto the fallback path
    resp = n._chat_rule_based("linkedin vs google ads")["response"]
    assert bp["linkedin"]["job_ad_cpc_range"] in resp
    assert bp["google_search_ads"]["average_cpc_all_industries"] in resp
    assert bp["google_search_ads"]["employment_services_cpc_estimated"] in resp

    resp2 = n._chat_rule_based("indeed vs ziprecruiter")["response"]
    assert bp["indeed"]["average_cpc_range"] in resp2
    assert bp["ziprecruiter"]["estimated_cpc_equivalent"] in resp2

    resp3 = n._chat_rule_based("indeed vs facebook")["response"]
    assert bp["meta_facebook_ads"]["global_median_cpc_all_objectives"] in resp3


# Maps each fallback summary to the KB by_platform entry its dollar figures
# must trace to. None = the platform deliberately has no KB entry (Glassdoor:
# job ads run on Indeed's CPC engine since the Sept-2025 consolidation), so
# its summary must carry no dollar figures at all.
_FALLBACK_KB_KEYS = {
    "Indeed": "indeed",
    "LinkedIn": "linkedin",
    "ZipRecruiter": "ziprecruiter",
    "Glassdoor": None,
    "Google Ads": "google_search_ads",
    "Meta/Facebook": "meta_facebook_ads",
}

# Magnitude suffixes ([kKmMbB]) are captured INTO the token so "$5K" must
# match "$5K" in the KB (it won't) rather than degrading to "$5" and
# false-passing against e.g. indeed's "$5.00+".
_DOLLAR_TOKEN = re.compile(
    r"\$\d[\d,]*(?:\.\d+)?(?:-\$\d[\d,]*(?:\.\d+)?)?\+?[kKmMbB]?"
)


def test_every_fallback_dollar_figure_traces_to_cited_kb_entry():
    """Generalized no-fabrication guard: every dollar figure in every
    hardcoded fallback summary must literally appear in the cited KB
    by_platform entry it stands in for; platforms without a KB entry must
    be number-free. Adding an uncited number to the fallback dict, or
    refreshing a KB band without updating the fallback, fails here --
    this is what makes the honesty contract permanent rather than a
    one-time cleanup.

    Documented residual blind spots (accepted: '$'-prefixed figures are
    the fabrication class that actually shipped before): non-'$' forms
    ('USD 5', bare '0.86'), percentages ('0.47%'), and malformed shapes
    ('$.50', '$ 5'). Provenance is platform-scoped, not field-scoped: a
    figure lifted from a different sub-field of the SAME platform's entry
    would pass -- the guard proves the number exists in the cited entry,
    not that the prose around it is semantically exact."""
    kb = _real_kb()
    bp = kb["benchmarks"]["cost_per_click"]["by_platform"]
    summaries = nova._PLATFORM_FALLBACK_SUMMARIES

    # A new platform added to the dict must be classified here first.
    assert set(summaries) == set(_FALLBACK_KB_KEYS), (
        "platform added/removed in _PLATFORM_FALLBACK_SUMMARIES without "
        "updating this test's KB-key map -- classify it (KB key or None) "
        "and cite its figures"
    )

    for platform, summary in summaries.items():
        tokens = _DOLLAR_TOKEN.findall(summary)
        kb_key = _FALLBACK_KB_KEYS[platform]
        if kb_key is None:
            assert not tokens, (
                f"{platform} has no KB entry -- its fallback summary must "
                f"carry no dollar figures, found: {tokens}"
            )
            continue
        haystack = json.dumps(bp[kb_key])
        for token in tokens:
            # (?!\d) so a truncated figure can't false-pass as a prefix of
            # a longer cited one (e.g. "$1.1" against Meta's "$1.05-$1.15").
            assert re.search(re.escape(token) + r"(?!\d)", haystack), (
                f"{platform} fallback figure {token!r} does not appear in "
                f"KB by_platform[{kb_key!r}] -- uncited or stale"
            )


def test_kb_entry_rendering_skips_nested_and_provenance_fields():
    """Served KB entries must not leak raw Python dict reprs (nested
    industry splits) or "_"-prefixed provenance keys into chat text."""
    n = _nova_with_kb(_real_kb())
    resp = n._chat_rule_based("linkedin vs google ads")["response"]
    assert "{'" not in resp and '{"' not in resp, "raw dict repr leaked"


def test_kb_entry_rendering_filter_unit():
    """Load-bearing unit check for the scalar/provenance filter: an entry
    fronted by a "_"-prefixed key and a nested dict must render only its
    scalar fields. Pre-fix, "_source" leaked as "-  Source: <url>" (the
    underscore title-cases to a leading space) and the nested dict leaked
    as a raw Python repr."""
    kb = {
        "benchmarks": {
            "cost_per_click": {
                "by_platform": {
                    "indeed": {
                        "_source": "https://example.test/cited",
                        "industry_cpc": {"tech": "$9.99"},
                        "average_cpc_range": "$0.25-$1.50",
                        "model": "CPC",
                    },
                    "linkedin": {"job_ad_cpc_range": "$1.50-$4.50"},
                }
            }
        }
    }
    n = _nova_with_kb(kb)
    resp = n._chat_rule_based("indeed vs linkedin")["response"]
    assert "$0.25-$1.50" in resp, "scalar fields beyond skipped keys not served"
    assert "$9.99" not in resp, "nested dict value leaked"
    assert "{'" not in resp, "raw dict repr leaked"
    assert "-  Source:" not in resp, "_source provenance key leaked"
    assert "https://example.test/cited" not in resp


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
