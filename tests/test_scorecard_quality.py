"""Regression tests for scorecard_generator.py channel-mix quality defects.

Covers two verified client-visible defects in the shareable HTML scorecard
(``scorecard_generator.generate_scorecard_html``):

  #17 -- capping at 10 channels then re-scaling the shown 10 to 100% made
         every displayed percentage disagree with its dollar amount AND
         with the workbook, while the header kept claiming the true (>10)
         channel count. Fixed by computing each row's percentage against
         the FULL plan total and folding channels past the cap into a
         single "Other (N channels)" row carrying its true combined share.
  #25 -- raw internal snake_case channel keys (``programmatic_dsp``,
         ``apac_regional``, ...) were printed verbatim on the public share
         page. Fixed by routing channel names through
         ``display_format.channel_label`` -- the same map ppt_generator.py
         and excel_v2.py already use -- so the scorecard never drifts from
         what the deck/workbook call a channel.

No network / LLM / Supabase is touched -- pure string builders over plain
dicts, same pattern as tests/test_pdf_scorecard.py.
"""

import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import scorecard_generator  # noqa: E402


def _twelve_channel_plan():
    """12 snake_case channels, uneven percentages -- mirrors the shape
    budget_engine.calculate_budget_allocation's channel_allocations produces.
    """
    channel_allocations = {
        "programmatic_dsp": {"dollar_amount": 20000, "percentage": 20.0},
        "niche_boards": {"dollar_amount": 15000, "percentage": 15.0},
        "global_boards": {"dollar_amount": 12000, "percentage": 12.0},
        "apac_regional": {"dollar_amount": 10000, "percentage": 10.0},
        "emea_regional": {"dollar_amount": 9000, "percentage": 9.0},
        "social_media": {"dollar_amount": 8000, "percentage": 8.0},
        "regional_boards": {"dollar_amount": 7000, "percentage": 7.0},
        "employer_branding": {"dollar_amount": 6000, "percentage": 6.0},
        "direct_hiring": {"dollar_amount": 5000, "percentage": 5.0},
        "lead_generation": {"dollar_amount": 4000, "percentage": 4.0},
        "referrals": {"dollar_amount": 2500, "percentage": 2.5},
        "staffing_agency": {"dollar_amount": 1500, "percentage": 1.5},
    }
    total_budget = sum(c["dollar_amount"] for c in channel_allocations.values())
    return {
        "_budget_allocation": {
            "metadata": {"total_budget": total_budget},
            "channel_allocations": channel_allocations,
        },
        "roles": ["Warehouse Associate"],
        "locations": ["Dallas, TX"],
        "industry": "Logistics",
    }, channel_allocations, total_budget


def _bar_rows(html: str) -> list[tuple[str, str]]:
    """Extract (name, "NN%...") pairs from the rendered channel bars."""
    return re.findall(
        r'<span style="font-size:14px;font-weight:500;color:[^;]+;">([^<]+)</span>\s*'
        r'<span style="font-size:13px;color:[^;]+;">([^<]+)</span>',
        html,
    )


# ---------------------------------------------------------------------------
# #17 -- true channel shares (percent must match dollar amount / full total)
# ---------------------------------------------------------------------------
def test_scorecard_more_than_10_channels_shows_other_row():
    """Beyond 10 channels, the scorecard must show a top-10 + one
    aggregated 'Other (N channels)' row -- never silently drop channels."""
    plan_data, channel_allocations, _total = _twelve_channel_plan()
    html = scorecard_generator.generate_scorecard_html(plan_data, "test-share-id")
    rows = _bar_rows(html)
    assert len(rows) == 11, f"expected top 10 + 1 Other row, got {len(rows)}: {rows}"
    assert rows[-1][0] == "Other (2 channels)", rows[-1]


def test_scorecard_displayed_percentages_match_dollar_amount_over_full_total():
    """Each displayed percentage must equal round(dollar_amount / FULL plan
    total * 100), not dollar_amount / sum-of-shown-10 * 100. Pre-fix, the
    10-channel re-scale inflated every shown row (e.g. programmatic_dsp's
    true 20% was shown as 21%)."""
    plan_data, channel_allocations, total_budget = _twelve_channel_plan()
    html = scorecard_generator.generate_scorecard_html(plan_data, "test-share-id")
    rows = _bar_rows(html)

    true_pct = {
        name: round(data["dollar_amount"] / total_budget * 100)
        for name, data in channel_allocations.items()
    }
    # programmatic_dsp is the top row and has an exact, unambiguous true share.
    top_name, top_info = rows[0]
    assert top_name in ("Programmatic (DSP)", "Programmatic DSP", "programmatic_dsp")
    shown_pct = int(re.match(r"(\d+)%", top_info).group(1))
    assert shown_pct == true_pct["programmatic_dsp"] == 20, (
        f"displayed {shown_pct}% for programmatic_dsp, true full-total share is "
        f"{true_pct['programmatic_dsp']}% ($20,000 / ${total_budget})"
    )


def test_scorecard_displayed_percentages_sum_to_100_over_all_money():
    """The shown rows (top 10 + Other) must sum to exactly 100 -- the Other
    row's share must be included, not just the top 10's re-scaled shares."""
    plan_data, _channel_allocations, _total = _twelve_channel_plan()
    html = scorecard_generator.generate_scorecard_html(plan_data, "test-share-id")
    rows = _bar_rows(html)
    total_pct = sum(int(re.match(r"(\d+)%", info).group(1)) for _name, info in rows)
    assert total_pct == 100, f"shown percentages summed to {total_pct}, not 100"


def test_scorecard_header_count_is_true_total_not_shown_count():
    """The 'Channels' stat card must keep reporting the TRUE total channel
    count even when only 10 + Other are rendered as bars."""
    plan_data, channel_allocations, _total = _twelve_channel_plan()
    html = scorecard_generator.generate_scorecard_html(plan_data, "test-share-id")
    m = re.search(r'Channels</div>\s*<div[^>]*>(\d+)</div>', html)
    assert m is not None
    assert int(m.group(1)) == len(channel_allocations) == 12


def test_scorecard_ten_or_fewer_channels_no_other_row():
    """The Other row must not appear when there are 10 or fewer channels."""
    channel_allocations = {
        "programmatic_dsp": {"dollar_amount": 50000, "percentage": 50.0},
        "niche_boards": {"dollar_amount": 50000, "percentage": 50.0},
    }
    plan_data = {
        "_budget_allocation": {
            "metadata": {"total_budget": 100000},
            "channel_allocations": channel_allocations,
        },
        "roles": ["Registered Nurse"],
        "locations": ["Dallas, TX"],
        "industry": "Healthcare",
    }
    html = scorecard_generator.generate_scorecard_html(plan_data, "test-share-id")
    assert "Other (" not in html


# ---------------------------------------------------------------------------
# #25 -- human channel labels, not raw snake_case keys
# ---------------------------------------------------------------------------
def test_scorecard_no_raw_snake_case_channel_keys_leak():
    """Raw internal keys (programmatic_dsp, apac_regional, ...) must never
    appear verbatim on the public share page."""
    plan_data, channel_allocations, _total = _twelve_channel_plan()
    html = scorecard_generator.generate_scorecard_html(plan_data, "test-share-id")
    for key in channel_allocations:
        assert f">{key}<" not in html, f"raw internal key {key!r} leaked into scorecard HTML"


def test_scorecard_channel_names_are_human_labels():
    """Known internal keys map to their human-facing labels via the shared
    display_format.channel_label map (reused, not reimplemented)."""
    plan_data, _channel_allocations, _total = _twelve_channel_plan()
    html = scorecard_generator.generate_scorecard_html(plan_data, "test-share-id")
    assert "Programmatic (DSP)" in html
    assert "APAC Regional" in html
    assert "EMEA Regional" in html
    assert "Niche / Industry Boards" in html
    assert "Global Job Boards" in html


def test_scorecard_already_human_names_are_not_mangled():
    """Names with no underscore (already human, e.g. from the legacy
    summary.channels shape) must pass through unchanged -- str.title()
    would otherwise turn 'LinkedIn' into 'Linkedin'."""
    plan_data = {
        "_budget_allocation": {
            "metadata": {"total_budget": 50000},
            "channel_allocations": {
                "LinkedIn": {"dollar_amount": 25000, "percentage": 50.0},
                "ZipRecruiter": {"dollar_amount": 25000, "percentage": 50.0},
            },
        },
        "roles": ["Registered Nurse"],
        "locations": ["Dallas, TX"],
        "industry": "Healthcare",
    }
    html = scorecard_generator.generate_scorecard_html(plan_data, "test-share-id")
    assert "LinkedIn" in html
    assert "Linkedin" not in html
    assert "ZipRecruiter" in html


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main([__file__, "-v"]))
