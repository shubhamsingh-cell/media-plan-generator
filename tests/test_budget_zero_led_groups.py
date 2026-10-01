"""Delta verifier on mpg-input-parity @5a59c1b (2026-10-01), item 4b: a
zero-led integer part was read as a magnitude nobody can know -- "01,500k"
planned $1,500, "010,000k" $10,000 and "₹07,620 lacs" ₹762,000. Only an
integer part of exactly "0" is a decimal ("0,750M" = 750,000); any other
zero-led integer is a 400 naming the field. (Python + node parity for the
same rows lives in the shared golden table, tests/fixtures/
budget_input_golden.json.)
"""

from __future__ import annotations

import pytest

import wizard_inputs
from tests.live_server import live_port, post_json  # noqa: F401 (fixture)


@pytest.mark.parametrize(
    "budget", ["01,500k", "010,000k", "₹07,620 lacs", "0050000", "$007,500", "05-10k"]
)
def test_zero_led_integers_are_400s_not_guesses(live_port, budget):  # noqa: F811
    status, body = post_json(live_port, "/api/estimate", {"budget_range": budget, "budget_only": True})
    assert status == 400 and body["field"] == "budget_range", body


@pytest.mark.parametrize(
    "budget,amount", [("0,750M", 750_000), ("0.5M", 500_000), ("0,5M", 500_000), (".5M", 500_000)]
)
def test_an_integer_part_of_exactly_zero_is_still_a_decimal(budget, amount):
    parsed = wizard_inputs.parse_budget_input(budget)
    assert parsed.ok and parsed.amount == amount
