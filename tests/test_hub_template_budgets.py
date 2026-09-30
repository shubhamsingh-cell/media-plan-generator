"""Hub "Jump into a template" cards open the wizard at the budget the card
states, and the preview and the plan read it the same (wizard audit D-03).

Pre-fix the links passed legacy bucket text: CDL (card "$50K") filled the
box with "< $50,000" (preview $0, "Set a budget"); Tech / Healthcare /
Retail (cards $150K / $100K / $80K) all passed "$50,000 - $250,000", which
the preview read as $50K and the server as the $150K midpoint -- a plan at
up to 3x what the preview showed.
"""

from __future__ import annotations

import json
import re
import urllib.parse
from pathlib import Path

import pytest

import wizard_inputs
from tests.wizard_js_harness import NODE, inputs_functions_js, run_node

_HUB = Path(__file__).resolve().parent.parent / "templates" / "hub.html"


def _template_cards() -> list[tuple[str, dict, float]]:
    """(role, query params, card's "Budget: $NK" figure) per template card."""
    html = _HUB.read_text(encoding="utf-8")
    cards = []
    for m in re.finditer(r'href="(/media-plan\?[^"]+)"', html):
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(m.group(1)).query))
        label = re.search(r"Budget: \$(\d+(?:\.\d+)?)([KM])", html[m.end() : m.end() + 6000])
        assert label, f"no 'Budget: $..' label after {m.group(1)}"
        figure = float(label.group(1)) * (1_000 if label.group(2) == "K" else 1_000_000)
        cards.append((params.get("role", ""), params, figure))
    return cards


CARDS = _template_cards()


def test_every_template_card_is_found():
    assert [c[0] for c in CARDS] == [
        "CDL Driver", "Software Engineer", "Registered Nurse", "Store Associate"
    ]


@pytest.mark.parametrize("role,params,figure", CARDS, ids=[c[0] for c in CARDS])
def test_link_budget_is_the_card_figure_as_a_plain_number(role, params, figure):
    budget = params.get("budget", "")
    assert re.fullmatch(r"\d+", budget), f"{role}: budget param {budget!r} is not a plain number"
    assert float(budget) == figure


@pytest.mark.parametrize("role,params,figure", CARDS, ids=[c[0] for c in CARDS])
def test_server_plans_the_card_figure(role, params, figure):
    plan = wizard_inputs.resolve_plan_budget(params["budget"], "campaign", "3 months")
    assert plan.ok and plan.total == figure


@pytest.mark.skipif(NODE is None, reason="node not installed")
def test_preview_reads_every_card_as_the_server_does():
    budgets = [c[1]["budget"] for c in CARDS]
    script = "\n".join(
        [
            inputs_functions_js(),
            "var b = " + json.dumps(budgets) + ";",
            "process.stdout.write(JSON.stringify(b.map(function (x) {",
            "  return novaResolvePlanBudget(x, 'campaign', '3 months').total; })));",
        ]
    )
    assert run_node(script) == [c[2] for c in CARDS]
