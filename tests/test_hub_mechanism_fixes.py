"""Hub mechanism-lens fixes (design-judge, 2026-10-17, prod e900b0f).

M1  the first-visit tour must not swallow the visitor's first click, must not
    scroll the page, must not point at a target below the fold, and its help
    step must link a help page the hub actually links.
M2  the city chips never moved the headline (every grid scenario is
    floor-clamped), so the card leads with the figures that do respond.
M3  chips expose their selection to assistive tech (aria-pressed).
M4  mini-stat labels are at least 11px.

Runs under pytest, or standalone:
``python3 -m pytest tests/test_hub_mechanism_fixes.py -q``.
"""

from __future__ import annotations

import html as html_lib
import json
import re
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402,F401  (registers the estimator with proof_claims)
import proof_claims  # noqa: E402
from tests.wizard_js_harness import NODE, run_node  # noqa: E402

HUB = PROJECT_ROOT / "templates" / "hub.html"
HUB_CSS = PROJECT_ROOT / "static" / "css" / "hub.css"


def _rendered() -> str:
    proof_claims.refresh()
    return proof_claims.render(HUB.read_text(encoding="utf-8"))


def _tour_script(src: str) -> str:
    start = src.index('if (localStorage.getItem("nova_onboarded")')
    return src[start : src.index("init();", start)]


# ── M1: the tour ─────────────────────────────────────────────────────────────


def test_m1_the_backdrop_passes_clicks_through_to_the_card() -> None:
    src = HUB.read_text(encoding="utf-8")
    rule = re.search(r"\.onboard-overlay\s*\{([^}]*)\}", src)
    assert rule, "tour overlay rule missing"
    assert re.search(r"pointer-events:\s*none", rule.group(1)), rule.group(1)
    assert "pointer-events: auto" not in rule.group(1)
    tooltip = re.search(r"\.onboard-tooltip\s*\{([^}]*)\}", src)
    assert tooltip and "pointer-events: none" not in tooltip.group(1)  # stays usable


def test_m1_the_tour_never_scrolls_and_dismisses_without_eating_the_click() -> None:
    tour = _tour_script(HUB.read_text(encoding="utf-8"))
    assert "scrollIntoView" not in tour
    # a capturing document listener, so the press that ends the tour still
    # reaches the element it landed on; the backdrop itself has no click handler
    assert re.search(
        r'addEventListener\(\s*"pointerdown",\s*dismissOutside,\s*true', tour
    )
    assert "overlay.addEventListener" not in tour
    assert "steps = steps.filter(onboardStepInView)" in tour
    # the tooltip is clamped into the viewport whatever its anchor
    assert "window.scrollY + window.innerHeight - th - 16" in tour


@pytest.mark.skipif(NODE is None, reason="node not installed")
def test_m1_a_step_below_the_fold_is_skipped() -> None:
    tour = _tour_script(HUB.read_text(encoding="utf-8"))
    fn = re.search(r"function onboardInView\(.*?\n        \}\n", tour, re.S)
    assert fn, "onboardInView missing"
    script = (
        fn.group(0)
        + """
const vw = 1440, vh = 900;
process.stdout.write(JSON.stringify({
  hero: onboardInView({ top: 93, right: 1300, left: 130 }, vw, vh),
  cardStepAtY1138: onboardInView({ top: 1138, right: 700, left: 100 }, vw, vh),
  justBelow: onboardInView({ top: 790, right: 700, left: 100 }, vw, vh),
  above: onboardInView({ top: -400, right: 700, left: 100 }, vw, vh),
  offRight: onboardInView({ top: 100, right: 2000, left: 1500 }, vw, vh),
}));
"""
    )
    out = run_node(script)
    assert out == {
        "hero": True,
        "cardStepAtY1138": False,
        "justBelow": False,
        "above": False,
        "offRight": False,
    }


def test_m1_the_help_step_links_the_guide_and_the_hub_links_it_too() -> None:
    html = _rendered()
    tour = _tour_script(html)
    assert 'href="/help/media-plans"' in tour and "help center" not in tour
    nav = re.search(r'<div class="nav-links">(.*?)</div>', html, re.S)
    assert nav and re.search(
        r'<a href="/help/media-plans"[^>]*>How to use</a>', nav.group(1)
    )
    footer = re.search(r"<footer.*?</footer>", html, re.S)
    assert footer and 'href="/help/media-plans">How to use</a>' in footer.group(0)


# ── M2: lead with what the chips move ────────────────────────────────────────


def test_m2_premise_cities_move_applications_not_the_headline() -> None:
    """Pins what the note under the chips says: for every role and budget the
    cost per hire and hires are the same in every city, while applications
    differ. If the engine ever changes that, the note must go with it."""
    grid = proof_claims.hub_pe_grid(force=True)
    assert grid
    moved_apps = 0
    for role in proof_claims.HUB_PE_ROLES:
        for budget in proof_claims.hub_pe_budgets():
            rows = [
                grid["scenarios"][
                    proof_claims.hub_pe_key(
                        proof_claims.hub_pe_brief(role, loc, budget)
                    )
                ]
                for loc in proof_claims.HUB_PE_LOCATIONS
            ]
            assert len({(r["est_cph"], r["est_hires"]) for r in rows}) == 1, (
                role,
                budget,
            )
            moved_apps += len({r["est_applications"] for r in rows}) > 1
    assert moved_apps > 0


def test_m2_the_card_leads_with_applications_then_cph_and_hires_follow() -> None:
    html = _rendered()
    card = html[html.index('id="plan-showcase"') :]
    card = card[: card.index('id="pe-default"')]
    order = [
        card.index(a)
        for a in ("data-pe-apps>", "data-pe-cpa>", "data-pe-hires>", "data-pe-cph>")
    ]
    assert order == sorted(order), order
    result = card[card.index('class="pe-result"') :]
    lead = re.search(r'<div class="pe-cph">(.*?)</div>\s*</div>', result, re.S)
    assert lead and "data-pe-apps>" in lead.group(1)
    assert "data-pe-cph>" not in lead.group(1)
    # the cost-per-hire basis label follows, still in visible text
    assert result.index("data-pe-cph>") < result.index("pe-cph-secondary")
    secondary = result[result.index("pe-cph-secondary") :]
    assert "media budget \u00f7 projected hires" in html_lib.unescape(secondary)
    # the line about the city chips is the first sentence of the one footnote
    foot = re.search(r'<div class="pe-footnote">(.*?)</div>', card, re.S)
    assert foot
    assert re.match(
        r"\s*<span data-pe-note>City changes (applications and )?channel mix"
        r"|\s*<span data-pe-note>City changes channel mix",
        foot.group(1),
    )
    assert "pe-chip-note" not in html  # no separate note line making the card taller


# ── M3: chips expose their state ─────────────────────────────────────────────


def test_m3_every_chip_has_aria_pressed_matching_its_selection() -> None:
    src = HUB.read_text(encoding="utf-8")
    chips = re.findall(
        r'<button\s[^>]*class="pe-chip[^"]*"[^>]*data-(?:role|loc)="\d"[^>]*>', src
    )
    assert len(chips) == len(proof_claims.HUB_PE_ROLES) + len(
        proof_claims.HUB_PE_LOCATIONS
    )
    for tag in chips:
        selected = "is-on" in tag
        assert f'aria-pressed="{str(selected).lower()}"' in tag, tag
    js = (PROJECT_ROOT / "static" / "js" / "hub.js").read_text(encoding="utf-8")
    assert js.count('setAttribute("aria-pressed"') == 2  # role + location handlers


# ── M4: label size ───────────────────────────────────────────────────────────


def test_m4_mini_stat_labels_are_at_least_11px_at_every_width() -> None:
    css = HUB_CSS.read_text(encoding="utf-8")
    sizes = []
    for m in re.finditer(r"\.pe-mini-label\s*\{([^}]*)\}", css):
        size = re.search(r"font-size:\s*([\d.]+)px", m.group(1))
        if size:
            sizes.append(float(size.group(1)))
    assert sizes, ".pe-mini-label font-size not found"
    assert min(sizes) >= 11, sizes
