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


def test_percent_range_is_one_bold() -> None:
    """Sweep: "Apply Rate: 3-6%" read "3-**6%**"."""
    assert fmt("Apply Rate: 3-6% for engineering") == "Apply Rate: **3-6%** for engineering"
    assert fmt("Apply rate 2.5–4.1% overall") == "Apply rate **2.5–4.1%** overall"


def test_no_template_placeholder_reaches_follow_ups() -> None:
    """Sweep: a salary answer offered "How does this compare to {nearby_city}?"."""
    for qtype in list(nova._FOLLOW_UP_MAP):
        for line in nova._generate_follow_ups("salary for a nurse in Houston", qtype):
            assert "{" not in line and "}" not in line, (qtype, line)


def test_plain_render_formats_forecast_money() -> None:
    out = nova._render_tool_data_plain(
        "CPC", {"forecast_30d": {"indeed": 1.31}, "currency": "EUR"}
    )
    assert "Indeed €1.31" in out


# Review 2026-11-03 (finding 12).
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Budget A$1,200 per month", "Budget **A$1,200** per month"),
        ("CPC NZ$2.10 and S$1.80", "CPC **NZ$2.10** and **S$1.80**"),
        ("Spend C$5,000-C$8,000", "Spend **C$5,000-C$8,000**"),
    ],
)
def test_prefixed_dollar_figures_are_bolded(raw: str, expected: str) -> None:
    assert fmt(raw) == expected


def test_urls_and_link_targets_untouched() -> None:
    raw = "See https://example.org/sale-50%-off and [the report](https://example.org/r?cut=5%&b=$10) now."
    out = fmt(raw)
    assert "https://example.org/sale-50%-off" in out
    assert "(https://example.org/r?cut=5%&b=$10)" in out


def test_berlin_plan_figures_say_us_dollars() -> None:
    """Transcript 27: a Berlin plan printed "$60,000" with nothing saying the
    figures are US dollars while the market's money is the euro."""
    nv = nova.Nova()
    data = nv._query_budget_projection(
        {"budget": 60000, "roles": ["Data Engineer"], "locations": ["Berlin"], "industry": "technology"}
    )
    out = nova._format_budget_response(data, 60000)
    import re

    assert not re.search(r"(?<![A-Za-z])\$\d", out), out[:600]
    assert "US$60,000" in out and "EUR" in out


def test_multi_country_spend_is_labelled_usd() -> None:
    """Transcript 13: "Monthly job ad spend: $1,500,000" beside "(currency: EUR)"."""
    res = nova.Nova()._chat_rule_based(
        "Compare hiring in Germany and France: job ad spend and boards"
    )
    text = res["response"]
    import re

    assert "Multi-Country Comparison" in text
    assert not re.search(r"spend: \*{0,2}\$\d", text), text[:800]
    assert "US$1,500,000" in text


def test_a_bare_world_city_names_its_market() -> None:
    data = nova.Nova()._query_budget_projection(
        {"budget": 60000, "roles": ["Data Engineer"], "locations": ["Berlin"], "industry": "technology"}
    )
    assert data["country"] == "Germany" and data.get("market_currency") == "EUR"
    assert data["currency"] == "USD"  # the tool still prices in US dollars
