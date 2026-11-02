"""One Sources footer per answer.

2026-11-03 chat sweep: every tool answer ended with the dated citations block
("**Sources** _(data freshness as of ...)_ - [1] ...") AND, after the
follow-ups, a second "**Sources:** ..." line from the enrichment pass.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import nova  # noqa: E402


def test_no_second_footer_after_the_citations_block() -> None:
    body = "CPC in Berlin is about €1.36 on Indeed."
    block = nova._build_citations_block(["Nova CPC Trend Tracker"])
    assert block
    for text in (body + block, nova._strip_t3_markers(body + block) if hasattr(nova, "_strip_t3_markers") else body + block):
        out, _ = nova._enrich_source_citations(text, ["track_cpc"], ["Nova CPC Trend Tracker"])
        assert out.count("Sources") == 1, out


def test_footer_still_added_when_there_is_none() -> None:
    out, _ = nova._enrich_source_citations("CPC is about $1.36.", ["track_cpc"], [])
    assert "**Sources:**" in out
