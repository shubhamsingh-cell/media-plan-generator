"""Nova's metric bolding never splits a number or doubles a bold.

Seen in the 2026-11-03 chat sweep: "*Budget Allocation: $500*" became
"**$50**0", an already-bold "**45%**" became "**4**5%**", "-5%" became
"-**5%**", "$209K" became "**$209**K", a range "$15-$35" gained a space
("$15- $35"), and percentages inside a ```json``` block were bolded.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import nova  # noqa: E402

fmt = nova._enrich_markdown_formatting


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("*Budget Allocation: $500*", "*Budget Allocation: **$500***"),
        ("Apply rate is **45%** overall", "Apply rate is **45%** overall"),
        ("Trend: -5% this month", "Trend: **-5%** this month"),
        ("LLM developers at $209K a year", "LLM developers at **$209K** a year"),
        ("High CPA range: $15-$35 per apply", "High CPA range: **$15-$35** per apply"),
        ("Cost per Application: $34.97–$58", "Cost per Application: **$34.97–$58**"),
        ("Monthly spend $1,500,000+", "Monthly spend **$1,500,000**+"),
        ("Budget $85,000, split two ways", "Budget **$85,000**, split two ways"),
        ("Rate fell 46.74% YoY", "Rate fell **46.74%** YoY"),
    ],
)
def test_bolds_whole_numbers_only(raw: str, expected: str) -> None:
    assert fmt(raw) == expected


def test_never_splits_a_bold_number() -> None:
    out = fmt("Totals: **$500** and **45%** and **$1,250,000**")
    assert out == "Totals: **$500** and **45%** and **$1,250,000**"


def test_code_blocks_and_inline_code_untouched() -> None:
    raw = '```json\n{"trend": "-5%", "cpc": "$1.36"}\n```\nInline `5%` stays.'
    out = fmt(raw)
    assert '"trend": "-5%"' in out and '"cpc": "$1.36"' in out
    assert "`5%`" in out


def test_idempotent() -> None:
    raw = "Spend $500 for 6% more, range $15-$35, trend -5%."
    once = fmt(raw)
    assert fmt(once) == once
    assert "****" not in once and "**$50**0" not in once


def test_plain_text_quality_pass_uses_the_same_rule() -> None:
    """_enrich_response_quality bolds plain-text replies (no markdown, > 200
    chars) and must not split a sign or suffix either."""
    body = (
        "The trend is -5% this quarter and the budget is $500 for one opening. "
        "Senior developers earn about $209K while apply rates sit near 6.41% for "
        "technology roles across the markets we track in this region today."
    )
    out = nova._enrich_response_quality({"response": body})["response"]
    assert "**-5%**" in out and "**$500**" in out and "**$209K**" in out
    assert "-**5%**" not in out and "**$209**K" not in out
