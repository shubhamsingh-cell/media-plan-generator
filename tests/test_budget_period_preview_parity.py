"""Regression: the wizard's live preview and /api/generate must turn the same
(amount, budget period, campaign duration) into the SAME campaign total.

Client report 2026-09-24 (The Hershey Company plan): "when I put budget in
it shows me what I want in the tool ($90k) but in the output it says $60k";
the server-side Slack notifier logged every run with ``Budget: $60,000``.

Root cause: the budget-period scaling (Per month / Per quarter / Per year ->
campaign total) had two independent implementations that disagreed:

  * the live preview (templates/partials/index/body_preview_js.html,
    ``gather()``) mapped the duration dropdown to midpoint months
    (``"3-6 months"`` -> 4.5) and scaled without a floor;
  * app.py's /api/generate handler took the FIRST integer in the duration
    string (``"3-6 months"`` -> 3, ``"1-2 years"`` -> 1 year, ``"Ongoing"``
    -> 1, ``"16 weeks"`` -> 16 *months*) and floored the multiplier at 1.

So $60,000 "Per quarter" over "3-6 months" previewed as $90K (60k x 4.5/3)
while the plan was generated for $60,000 (60k x max(3/3, 1)) -- exactly the
reported 90k -> 60k. Other options diverged too (exact "2 weeks".."3 months"
were missing from the preview map, so it silently used 6 months).

These tests execute the REAL server block (extracted from app.py source and
run against app's own globals) and the REAL preview arithmetic (extracted
from the template and run under node) for every duration option in the
wizard's <select>, and require the two to agree.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402

_PREVIEW_JS = PROJECT_ROOT / "templates" / "partials" / "index" / "body_preview_js.html"
_BODY_CONTENT = PROJECT_ROOT / "templates" / "partials" / "index" / "body_content.html"
_NODE = shutil.which("node")


def _wizard_duration_options() -> list:
    """Every non-empty <option value> of the wizard's #campaignDuration."""
    src = _BODY_CONTENT.read_text()
    start = src.index('id="campaignDuration"')
    end = src.index("</select>", start)
    return [v for v in re.findall(r'<option value="([^"]*)"', src[start:end]) if v]


def _server_campaign_budget(amount: str, period: str, duration: str) -> float:
    """Run app.py's own budget-period block on a request dict and return
    the campaign budget it hands to generation (what the Slack notifier,
    Excel and deck all read)."""
    src = (PROJECT_ROOT / "app.py").read_text()
    start = src.index("# ── Budget period normalization (monthly/quarterly/annual")
    start = src.rindex("\n", 0, start) + 1  # include the line's indentation
    end = src.index("# ── Gold Standard: Campaign start month validation", start)
    end = src.rindex("\n", 0, end) + 1
    block = textwrap.dedent(src[start:end])
    data = {
        "budget_range": amount,
        "budget_period": period,
        "campaign_duration": duration,
    }
    ns = dict(vars(app))
    ns["data"] = data
    exec(compile(block, "app.py<budget-period-block>", "exec"), ns)
    return app.parse_budget(str(data.get("budget") or data.get("budget_range") or ""))


def _preview_campaign_total(base: float, period: str, duration: str) -> float:
    """Run the preview's own gather() budget arithmetic under node."""
    src = _PREVIEW_JS.read_text()
    # The preview's module-level duration/period tables live between the
    # DURATION_MONTHS declaration and the "Small helpers" section.
    dm_start = src.index("var DURATION_MONTHS = {")
    dm_end = src.index("// ── Small helpers", dm_start)
    g_start = src.index('var months = DURATION_MONTHS[val("campaignDuration")]')
    g_end = src.index("// channels", g_start)
    script = "\n".join(
        [
            src[dm_start:dm_end],
            "function val(id) { return ({campaignDuration: "
            + json.dumps(duration)
            + ", budgetPeriod: "
            + json.dumps(period)
            + "})[id] || ''; }",
            f"var base = {base!r};",
            src[g_start:g_end],
            "process.stdout.write(String(total));",
        ]
    )
    out = subprocess.run(
        [_NODE, "-e", script], capture_output=True, text=True, timeout=30, check=True
    )
    return float(out.stdout)


needs_node = pytest.mark.skipif(_NODE is None, reason="node not installed")


@needs_node
def test_hershey_quarterly_60k_over_3_6_months_generates_what_preview_showed():
    # The reported case: preview hero reads $90K, output must be $90,000.
    preview = _preview_campaign_total(60000.0, "quarterly", "3-6 months")
    server = _server_campaign_budget("60,000", "quarterly", "3-6 months")
    assert preview == 90000.0
    assert server == preview, (
        f"preview showed ${preview:,.0f} but /api/generate used ${server:,.0f}"
    )


@needs_node
@pytest.mark.parametrize("period", ["campaign", "monthly", "quarterly", "annual"])
def test_every_duration_option_scales_identically_in_preview_and_server(period):
    mismatches = []
    for duration in _wizard_duration_options():
        preview = round(_preview_campaign_total(20000.0, period, duration))
        server = round(_server_campaign_budget("$20,000", period, duration))
        if preview != server:
            mismatches.append(f"{duration!r}: preview ${preview:,} vs server ${server:,}")
    assert not mismatches, f"period={period}: " + "; ".join(mismatches)


def test_preview_duration_map_covers_every_wizard_option():
    src = _PREVIEW_JS.read_text()
    dm_start = src.index("var DURATION_MONTHS = {")
    dm_end = src.index("};", dm_start)
    literal = src[dm_start:dm_end]
    missing = [d for d in _wizard_duration_options() if f'"{d}"' not in literal]
    assert not missing, f"preview DURATION_MONTHS lacks wizard options: {missing}"


def test_server_duration_map_matches_preview_map():
    src = _PREVIEW_JS.read_text()
    dm_start = src.index("var DURATION_MONTHS = {")
    dm_end = src.index("};", dm_start)
    js_pairs = dict(
        (k, float(v))
        for k, v in re.findall(r'"([^"]+)"\s*:\s*([0-9.]+)', src[dm_start:dm_end])
    )
    assert {k: float(v) for k, v in app.BUDGET_DURATION_MONTHS.items()} == js_pairs


def test_free_text_weeks_are_not_read_as_months():
    # API callers: "16 weeks" used to become a 16-MONTH multiplier.
    assert _server_campaign_budget("$10,000", "monthly", "16 weeks") < 40000
