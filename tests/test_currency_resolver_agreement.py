"""Regression test for TASK 1 (2026-09-24): one plan-currency resolver.

``plan_currency.currency_for_plan_with_basis`` is THE canonical plan-level
currency resolver (declare-not-convert: explicit field > symbol the client
typed > markets only if unanimous > USD). ``ppt_generator._plan_currency_code``
and ``scorecard_generator._currency_symbol`` already delegated to it.
``excel_v2._plan_currency_code`` and ``bundle_qa._resolve_plan_currency`` used
to reimplement the same logic by hand; when the three drifted, correct
bundles failed with ~50 false ``currency_symbol_mixing`` criticals because
the workbook/gate no longer agreed with the deck on the plan's currency.

This test asserts, over a matrix of >=12 plans, that ALL FOUR call sites
agree on the resolved currency:
  - ppt_generator._plan_currency_code   (the deck)
  - excel_v2._plan_currency_code        (the workbook)
  - scorecard_generator._currency_symbol (the scorecard)
  - bundle_qa._resolve_plan_currency    (the delivery gate)

Run standalone: ``python3 tests/test_currency_resolver_agreement.py``
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pytest  # noqa: E402

import bundle_qa  # noqa: E402
import excel_v2  # noqa: E402
import plan_currency as pc  # noqa: E402
import ppt_generator as ppt  # noqa: E402
import scorecard_generator as sc  # noqa: E402


# Each case: (id, plan_data). Covers: explicit currency_code; typed symbols
# ("£"/"$"/"₹"/"A$"/"¥") with CONTRADICTING locations (declare-not-convert
# must win); unanimous markets; mixed/disagreeing markets; US-only; dict
# locations; "City, Country" string locations.
CASES: list[tuple[str, dict]] = [
    (
        "explicit_currency_code_field",
        {"currency_code": "GBP", "locations": ["New York, United States"]},
    ),
    (
        "explicit_currency_field_lowercase_country",
        {"currency": "eur", "locations": ["Austin, TX"]},
    ),
    (
        "typed_gbp_symbol_contradicting_us_location",
        {"budget": "£50,000 total", "locations": ["Austin, TX"]},
    ),
    (
        "typed_usd_symbol_contradicting_uk_location",
        {"budget": "$120,000", "locations": ["London, United Kingdom"]},
    ),
    (
        "typed_inr_symbol_contradicting_us_location",
        {"budget": "₹9,00,000 total", "locations": ["Chicago, IL"]},
    ),
    (
        "typed_aud_symbol_contradicting_uk_location",
        {"budget": "A$80,000", "locations": ["London, United Kingdom"]},
    ),
    (
        "typed_jpy_symbol_contradicting_us_location",
        {"budget": "¥5,000,000", "locations": ["Dallas, TX"]},
    ),
    (
        "unanimous_markets_uk",
        {"locations": ["London, United Kingdom", "Manchester, United Kingdom"]},
    ),
    (
        "mixed_markets_uk_and_us_no_declaration",
        {"locations": ["London, United Kingdom", "New York, United States"]},
    ),
    (
        "us_only_locations",
        {"locations": ["Dallas, TX", "Chicago, IL"]},
    ),
    (
        "dict_locations_india",
        {"locations": [{"city": "Mumbai", "country": "India"}]},
    ),
    (
        "city_country_string_locations_spain",
        {"locations": ["Madrid, Spain"]},
    ),
    (
        "no_currency_no_locations_defaults_usd",
        {},
    ),
    (
        "primary_location_field_only_germany",
        {"primary_location": "Berlin, Germany"},
    ),
    (
        "us_state_guard_denver_not_colombia",
        {"locations": ["Denver, CO"]},
    ),
]


class TestAllFourResolversAgree:
    @pytest.mark.parametrize("case_id,data", CASES, ids=[c[0] for c in CASES])
    def test_deck_workbook_scorecard_gate_agree(self, case_id, data):
        canonical_code, _basis = pc.currency_for_plan_with_basis(dict(data))

        deck_code = ppt._plan_currency_code(dict(data))
        workbook_code = excel_v2._plan_currency_code(dict(data))
        scorecard_symbol = sc._currency_symbol(dict(data))
        gate_code, gate_symbol = bundle_qa._resolve_plan_currency(dict(data))

        assert deck_code == canonical_code, (case_id, "deck", deck_code)
        assert workbook_code == canonical_code, (case_id, "workbook", workbook_code)
        assert gate_code == canonical_code, (case_id, "gate", gate_code)
        assert scorecard_symbol == pc.symbol_for_code(canonical_code), (
            case_id,
            "scorecard",
            scorecard_symbol,
        )
        assert gate_symbol == pc.symbol_for_code(canonical_code), (
            case_id,
            "gate_symbol",
            gate_symbol,
        )

        # All four agree with EACH OTHER, not just with the canonical
        # resolver -- the actual failure mode this guards against.
        assert deck_code == workbook_code == gate_code, case_id


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
