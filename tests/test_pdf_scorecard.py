"""Tests for the PDF report + shareable scorecard P1 polish.

Covers the four generators in the pdf_scorecard unit:
  * scorecard_generator.generate_scorecard_html   (shareable HTML scorecard)
  * pdf_generator.generate_plan_html_report       (print-mode HTML report)
  * pdf_report.generate_pdf_report                (reportlab PDF, if installed)

No network / LLM / Supabase is touched -- these generators are pure string
builders over a plain dict. Verifies:
  - currency precision is consistent (whole-dollar for large, 2dp for per-unit)
  - trust / sourcing / freshness signals appear on the scorecard
  - channel-mix percentages round-and-remainder to total exactly 100
  - the HTML report carries a print-mode footer (date + page counter)
  - empty channel-mix renders a friendly message, not a stranded 100% total
  - no "N/A" / "None" placeholder leaks
  - og:image + static logo refs point at files that exist on disk

Runs under pytest, or standalone: ``python3 tests/test_pdf_scorecard.py``.
"""

import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pdf_generator  # noqa: E402
import scorecard_generator  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures (plain dicts -- no external data)
# ---------------------------------------------------------------------------
def _three_even_channels_report_plan():
    """3 channels at 33.3% each -> raw percentages do NOT sum to 100."""
    return {
        "budget": 50000,
        "industry": "Healthcare",
        "roles": ["Registered Nurse"],
        "locations": ["Dallas, TX"],
        "channels": [
            {
                "name": "Indeed",
                "allocation_pct": 33.3,
                "spend": 16650,
                "cpc": 1.2,
                "cpa": 4.2,
                "projected_clicks": 13875,
                "projected_applies": 3964,
                "projected_hires": 40,
            },
            {
                "name": "LinkedIn",
                "allocation_pct": 33.3,
                "spend": 16650,
                "cpc": 3.5,
                "cpa": 12.0,
                "projected_clicks": 4757,
                "projected_applies": 1387,
                "projected_hires": 14,
            },
            {
                "name": "ZipRecruiter",
                "allocation_pct": 33.3,
                "spend": 16700,
                "cpc": 0.95,
                "cpa": 3.8,
                "projected_clicks": 17578,
                "projected_applies": 4394,
                "projected_hires": 44,
            },
        ],
    }


def _three_even_scorecard_plan():
    return {
        "_budget_allocation": {
            "metadata": {"total_budget": 50000},
            "channel_allocations": {
                "Indeed": {"dollar_amount": 16650, "percentage": 33.3},
                "LinkedIn": {"dollar_amount": 16650, "percentage": 33.3},
                "ZipRecruiter": {"dollar_amount": 16700, "percentage": 33.3},
            },
        },
        "roles": ["Registered Nurse"],
        "locations": ["Dallas, TX"],
        "industry": "Healthcare",
    }


def _no_placeholder_leaks(html: str) -> bool:
    """True when no 'N/A' / 'None' placeholder text leaks into the output.

    Excludes legitimate substrings (e.g. CSS 'none', the word 'None' inside
    attributes) by only flagging the visible-cell patterns we control.
    """
    bad = ["N/A", ">None<", "None%", "$None", ">N/A<"]
    return not any(b in html for b in bad)


# ---------------------------------------------------------------------------
# scorecard_generator: percentage normalization (round-and-remainder)
# ---------------------------------------------------------------------------
def test_normalize_percentages_sums_to_100():
    chs = [
        {"name": "A", "percentage": 33.3, "dollar_amount": 0},
        {"name": "B", "percentage": 33.3, "dollar_amount": 0},
        {"name": "C", "percentage": 33.3, "dollar_amount": 0},
    ]
    scorecard_generator._normalize_percentages(chs)
    pcts = [c["display_pct"] for c in chs]
    assert sum(pcts) == 100
    # largest-remainder pushes the spare unit to one channel: 34/33/33
    assert sorted(pcts, reverse=True) == [34, 33, 33]


def test_normalize_percentages_single_channel_is_100():
    chs = [{"name": "Indeed", "percentage": 100, "dollar_amount": 50000}]
    scorecard_generator._normalize_percentages(chs)
    assert chs[0]["display_pct"] == 100


def test_normalize_percentages_zero_total_is_all_zero():
    chs = [
        {"name": "A", "percentage": 0, "dollar_amount": 0},
        {"name": "B", "percentage": 0, "dollar_amount": 0},
    ]
    scorecard_generator._normalize_percentages(chs)
    assert [c["display_pct"] for c in chs] == [0, 0]


def test_normalize_percentages_already_clean_unchanged():
    chs = [
        {"name": "A", "percentage": 60, "dollar_amount": 0},
        {"name": "B", "percentage": 40, "dollar_amount": 0},
    ]
    scorecard_generator._normalize_percentages(chs)
    assert [c["display_pct"] for c in chs] == [60, 40]


def test_normalize_percentages_empty_list_no_error():
    chs = []
    scorecard_generator._normalize_percentages(chs)
    assert chs == []


# ---------------------------------------------------------------------------
# scorecard_generator: full HTML render
# ---------------------------------------------------------------------------
def test_scorecard_channel_labels_total_100():
    html = scorecard_generator.generate_scorecard_html(
        _three_even_scorecard_plan(), "abc123"
    )
    # channel bar labels render as whole-number "NN%"
    labels = [int(x) for x in re.findall(r">(\d+)%", html)]
    assert sum(labels) == 100
    assert sorted(labels, reverse=True) == [34, 33, 33]


def test_scorecard_has_trust_signals():
    html = scorecard_generator.generate_scorecard_html(
        _three_even_scorecard_plan(), "abc123"
    )
    # generation date
    assert "Generated" in html
    assert re.search(r"<time datetime=\"\d{4}-\d{2}-\d{2}\">", html)
    # one-line methodology / data-basis note
    assert "Data basis" in html or "AI media planner" in html
    # footer / attribution
    assert "<footer" in html
    assert "Nova AI Suite" in html


def test_scorecard_no_placeholder_leaks():
    html = scorecard_generator.generate_scorecard_html(
        _three_even_scorecard_plan(), "abc123"
    )
    assert _no_placeholder_leaks(html)


def test_scorecard_static_refs_exist_on_disk():
    html = scorecard_generator.generate_scorecard_html(
        _three_even_scorecard_plan(), "abc123"
    )
    # og:image must point at static/og-scorecard.png which must exist
    assert "/static/og-scorecard.png" in html
    assert (PROJECT_ROOT / "static" / "og-scorecard.png").is_file()
    # the brand logo file referenced across the unit also exists
    assert (PROJECT_ROOT / "static" / "joveo-logo.png").is_file()


def test_scorecard_empty_channels_friendly_message():
    html = scorecard_generator.generate_scorecard_html(
        {"roles": ["RN"], "locations": ["Dallas"], "industry": "Healthcare"},
        "empty1",
    )
    assert "Channel mix coming soon" in html
    # no stranded data percentage label (CSS "100%" widths are fine, but there
    # must be no "NN%" channel-bar label rendered for a zero-channel plan)
    bar_labels = re.findall(r"<span[^>]*>(\d+)%</span>", html)
    assert bar_labels == []
    assert _no_placeholder_leaks(html)


# ---------------------------------------------------------------------------
# pdf_generator: print-mode HTML report
# ---------------------------------------------------------------------------
def test_report_allocation_column_totals_100():
    html = pdf_generator.generate_plan_html_report(
        _three_even_channels_report_plan(), "Acme Health", "Healthcare"
    )
    # the Allocation cells are whole-number "NN%" in <td class="num">
    cells = [int(x) for x in re.findall(r'<td class="num">(\d+)%</td>', html)]
    # 3 channel rows + the Total footer (100)
    assert cells.count(100) >= 1  # Total row
    channel_cells = [c for c in cells if c != 100]
    assert sum(channel_cells) == 100


def test_report_currency_precision_consistent():
    html = pdf_generator.generate_plan_html_report(
        _three_even_channels_report_plan(), "Acme Health", "Healthcare"
    )
    # large values render whole-dollar (no cents): $16,650 not $16,650.00
    assert "$16,650" in html
    assert "$16,650.00" not in html
    # per-unit values keep 2dp: CPC $1.20, CPA $4.20
    assert "$1.20" in html
    assert "$4.20" in html


def test_report_has_print_footer():
    html = pdf_generator.generate_plan_html_report(
        _three_even_channels_report_plan(), "Acme Health", "Healthcare"
    )
    # running @page footer: date (left) + page x of y (right)
    assert "@bottom-left" in html
    assert "@bottom-right" in html
    assert "counter(page)" in html
    assert "counter(pages)" in html


def test_report_no_placeholder_leaks():
    html = pdf_generator.generate_plan_html_report(
        _three_even_channels_report_plan(), "Acme Health", "Healthcare"
    )
    assert _no_placeholder_leaks(html)


def test_report_missing_channel_name_uses_dash_not_na():
    plan = {
        "budget": 1000,
        "channels": [{"allocation_pct": 100, "spend": 1000}],  # no name key
    }
    html = pdf_generator.generate_plan_html_report(plan, "Acme", "Tech")
    assert "N/A" not in html
    assert "&mdash;" in html  # em dash placeholder for the missing name


def test_report_empty_channels_friendly_message():
    plan = {"budget": 50000, "channels": []}
    html = pdf_generator.generate_plan_html_report(plan, "Acme", "Tech")
    assert "No channel allocation yet" in html
    # no stranded "100%" total over an empty table
    assert '<td class="num">100%</td>' not in html
    assert _no_placeholder_leaks(html)


def test_report_static_logo_ref_exists():
    html = pdf_generator.generate_plan_html_report(
        _three_even_channels_report_plan(), "Acme Health", "Healthcare"
    )
    refs = re.findall(r'(?:src|href|content)="(/static/[^"]+)"', html)
    for ref in refs:
        on_disk = PROJECT_ROOT / ref.lstrip("/")
        assert on_disk.is_file(), f"static ref {ref} missing on disk"


# ---------------------------------------------------------------------------
# pdf_report: reportlab PDF (skipped gracefully if reportlab absent)
# ---------------------------------------------------------------------------
def test_pdf_report_builds_valid_pdf_bytes():
    try:
        import reportlab  # noqa: F401
    except ImportError:
        return  # reportlab optional -- skip when not installed
    import pdf_report

    data = pdf_report.generate_pdf_report(
        _three_even_channels_report_plan(), "Acme Health", "Healthcare"
    )
    assert isinstance(data, (bytes, bytearray))
    assert data[:5] == b"%PDF-"  # valid PDF magic header
    assert len(data) > 1000


def test_pdf_report_empty_channels_does_not_crash():
    try:
        import reportlab  # noqa: F401
    except ImportError:
        return
    import pdf_report

    data = pdf_report.generate_pdf_report(
        {"budget": 50000, "channels": []}, "Acme", "Tech"
    )
    assert isinstance(data, (bytes, bytearray))
    assert data[:5] == b"%PDF-"


if __name__ == "__main__":
    _failures = 0
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            try:
                _fn()
                print(f"PASS {_name}")
            except AssertionError as exc:
                _failures += 1
                print(f"FAIL {_name}: {exc}")
            except Exception as exc:  # noqa: BLE001
                _failures += 1
                print(f"ERROR {_name}: {exc!r}")
    sys.exit(1 if _failures else 0)
