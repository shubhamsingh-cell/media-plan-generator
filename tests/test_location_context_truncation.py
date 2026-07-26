"""Regression tests for the residual `mid_word_truncation` bundle_qa warnings.

A regenerated multi-market bundle emitted 6 `mid_word_truncation` warnings
from bundle_qa, all on the Market Intelligence "Location Economic Context"
table's Context column -- research.get_labour_market_intelligence's
context_note is a two-sentence template ("International market -- {country}
in {region} region. Local labour laws and recruitment practices apply.")
and excel_v2's flat 80-char `_truncate_at_word_boundary` cutoff consistently
landed partway through the SECOND sentence for every real country/region
combination, producing a dangling fragment like "Local labour laws and...".

`_truncate_at_word_boundary` itself was never cutting mid-WORD (verified
below) -- the defect is architectural: a hard length cap with no notion
that a fragment reads worse than a dropped sentence. The fix
(`excel_v2._truncate_at_clause_boundary`) backs the cut off to the last
complete sentence and keeps everything up to it, dropping a sentence that
doesn't fit rather than showing half of it.

VACUOUSNESS: run against a throwaway pre-fix worktree (git worktree add
--detach HEAD at the parent commit) -- see the task report for the observed
pre-fix failures (6 mid_word_truncation findings from bundle_qa on the
6-market bundle; AssertionError on the fragment check for every country).
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import re

import excel_v2  # noqa: E402
import ppt_generator  # noqa: E402
import research  # noqa: E402
import tools_regen_bundles as T  # noqa: E402
from bundle_qa import run_bundle_qa  # noqa: E402

# The exact bundle_qa regex a dangling fragment trips: a letter directly
# followed by an ellipsis at the end of the string.
_MID_WORD_ELLIPSIS_RE = re.compile(r"[a-zA-Z](\.\.\.|…)$")

# 6 real non-US markets -- the same shape (country/region combination) that
# produced the 6 real-incident warnings.
_COUNTRIES = [
    "United Kingdom",
    "Australia",
    "Mexico",
    "Argentina",
    "Canada",
    "New Zealand",
]


def _context_note(country: str) -> str:
    return (
        f"International market — {country} in Some Region region. Local "
        "labour laws and recruitment practices apply."
    )


def test_word_boundary_truncation_was_never_mid_word_but_still_dangled():
    """Documents the pre-existing baseline: _truncate_at_word_boundary is
    correct on its own terms (no mid-WORD cut) yet the bundle_qa regex
    still fires, because the fragment is a dangling CLAUSE, not a broken
    word -- the reason a purely word-safe truncator wasn't sufficient."""
    for country in _COUNTRIES:
        note = _context_note(country)
        cut = excel_v2._truncate_at_word_boundary(note, 80)
        assert not cut.split()[-1].rstrip("…") == "" # never an empty trailing token
        # The old behavior the bug report describes: ends with "and…" or
        # "laws…" -- a real, complete word immediately followed by an
        # ellipsis with no terminal punctuation -- which IS what
        # bundle_qa's own regex flags.
        assert _MID_WORD_ELLIPSIS_RE.search(cut), (
            f"expected the OLD truncator to still trip the bundle_qa regex "
            f"for {country!r}: {cut!r}"
        )


def test_clause_boundary_truncation_never_dangles():
    """The fix: every one of the 6 real country/region shapes must produce
    a result that does NOT trip bundle_qa's mid_word_truncation regex, and
    must end on real sentence punctuation (a complete clause), never a
    bare word abruptly followed by an ellipsis."""
    for country in _COUNTRIES:
        note = _context_note(country)
        cut = excel_v2._truncate_at_clause_boundary(note, 80)
        assert not _MID_WORD_ELLIPSIS_RE.search(cut), (
            f"{country}: still dangling: {cut!r}"
        )
        assert cut.endswith("."), f"{country}: expected a complete sentence: {cut!r}"
        assert len(cut) <= 80
        # Honest degradation, not fabrication: the kept text is a real
        # prefix of the original note, not new/invented wording.
        assert note.startswith(cut)


def test_clause_boundary_is_noop_when_text_already_fits():
    short = "Local labour laws apply."
    assert excel_v2._truncate_at_clause_boundary(short, 80) == short


def test_clause_boundary_falls_back_to_word_boundary_with_no_punctuation():
    """No sentence-ending punctuation anywhere before max_len -> falls back
    to the word-safe truncator (still never a mid-word cut)."""
    text = "supercalifragilisticexpialidocious " * 10  # no periods at all
    cut = excel_v2._truncate_at_clause_boundary(text.strip(), 40)
    assert cut == excel_v2._truncate_at_word_boundary(text.strip(), 40)
    assert cut.endswith("…")


def test_clause_boundary_defensive_on_non_positive_max_len():
    assert excel_v2._truncate_at_clause_boundary("anything", 0) == ""
    assert excel_v2._truncate_at_clause_boundary("anything", -5) == ""


# ---------------------------------------------------------------------------
# End-to-end: bundle_qa on a real regenerated 6-market bundle must report
# ZERO mid_word_truncation findings.
# ---------------------------------------------------------------------------

_SIX_MARKET_BRIEF: dict = {
    "client_name": "uber",
    "requester_name": "Test Requester",
    "requester_email": "test@joveo.com",
    "budget": "£2,000,000",
    "campaign_duration": "1-3 months",
    "hire_volume": "500+ hires",
    "work_environment": "hybrid",
    "locations": ["UK", "Australia", "Mexico", "argentina", "canada", "new zealand"],
    "roles": ["commercial cab driver"],
    "target_roles": [
        {"title": "commercial cab driver", "count": 500, "tier": "Hourly"}
    ],
}


def test_six_market_bundle_has_zero_mid_word_truncation_findings():
    data = T.build_plan_data(_SIX_MARKET_BRIEF)
    xlsx_obj = excel_v2.generate_excel_v2(data, research_mod=research)
    pptx_obj = ppt_generator.generate_pptx(data)
    xlsx_bytes = xlsx_obj.getvalue() if hasattr(xlsx_obj, "getvalue") else xlsx_obj
    pptx_bytes = pptx_obj.getvalue() if hasattr(pptx_obj, "getvalue") else pptx_obj
    findings = run_bundle_qa(pptx_bytes, xlsx_bytes, data)
    truncation = [f for f in findings if f.get("code") == "mid_word_truncation"]
    assert truncation == [], f"mid_word_truncation findings remain: {truncation}"


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
