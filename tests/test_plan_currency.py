"""Tests for plan_currency + intl_benchmark_lookup currency localization.

Covers backlog Q4 (Cyrillic RUB/UAH support) and Q5 (non-US currency display
in generated media plans).
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from plan_currency import (  # noqa: E402
    currency_for_country,
    format_money,
    is_non_usd,
    symbol_for_code,
)


class TestCurrencyForCountry:
    @pytest.mark.parametrize(
        "country,expected",
        [
            ("United States", "USD"),
            ("US", "USD"),
            ("United Kingdom", "GBP"),
            ("UK", "GBP"),
            ("London, UK", "GBP"),
            ("Germany", "EUR"),
            ("France", "EUR"),
            ("Ireland", "EUR"),
            ("India", "INR"),
            ("Japan", "JPY"),
            ("Canada", "CAD"),
            ("Australia", "AUD"),
            ("Brazil", "BRL"),
            ("Mexico", "MXN"),
            ("Singapore", "SGD"),
            ("UAE", "AED"),
            ("Russia", "RUB"),
            ("Ukraine", "UAH"),
        ],
    )
    def test_known_countries(self, country, expected):
        assert currency_for_country(country) == expected

    @pytest.mark.parametrize("country", ["Atlantis", "Narnia", "", None])
    def test_unknown_countries_return_none(self, country):
        assert currency_for_country(country) is None

    def test_no_false_substring_match(self):
        # "antarctica" must NOT match the "ca"/"in" short aliases
        assert currency_for_country("Antarctica") is None


class TestSymbolForCode:
    @pytest.mark.parametrize(
        "code,symbol",
        [
            ("USD", "$"),
            ("GBP", "£"),
            ("EUR", "€"),
            ("INR", "₹"),
            ("JPY", "¥"),
            ("RUB", "₽"),  # Cyrillic (Q4)
            ("UAH", "₴"),  # Cyrillic (Q4)
        ],
    )
    def test_symbols(self, code, symbol):
        assert symbol_for_code(code) == symbol

    def test_unknown_code_falls_back_to_code_prefix(self):
        # Unknown ISO code renders as "XYZ " so the value is never unlabeled
        assert symbol_for_code("XYZ").strip() == "XYZ"

    def test_none_defaults_to_dollar(self):
        assert symbol_for_code(None) == "$"


class TestFormatMoney:
    def test_gbp_whole(self):
        assert format_money(28407, "GBP") == "£28,407"

    def test_eur_decimal(self):
        assert format_money(7.5, "EUR") == "€7.50"

    def test_inr_large(self):
        assert format_money(300000, "INR") == "₹300,000"

    def test_rub_cyrillic(self):
        assert format_money(50, "RUB") == "₽50"

    def test_uah_cyrillic(self):
        assert format_money(1200, "UAH") == "₴1,200"

    def test_none_value_is_na(self):
        assert format_money(None, "USD") == "N/A"

    def test_bool_is_na(self):
        # bool is an int subclass -- must not render as money
        assert format_money(True, "USD") == "N/A"

    def test_default_code_is_usd(self):
        assert format_money(100) == "$100"


class TestIsNonUsd:
    def test_uk_is_non_usd(self):
        assert is_non_usd("United Kingdom") is True

    def test_us_is_not_non_usd(self):
        assert is_non_usd("United States") is False

    def test_unknown_is_not_non_usd(self):
        assert is_non_usd("Atlantis") is False


class TestLocalSalarySummary:
    """Integration with the real intl_role_benchmarks_v1.json dataset."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from intl_benchmark_lookup import get_local_salary_summary, is_available

        if not is_available():
            pytest.skip("intl_role_benchmarks_v1.json not available")
        self.fn = get_local_salary_summary

    def test_uk_healthcare_is_gbp(self):
        s = self.fn("Healthcare", "United Kingdom")
        assert s is not None
        assert s["currency"] == "GBP"
        assert s["local_display"].startswith("£")
        assert "usd_display" in s  # non-USD -> includes USD equivalent

    def test_germany_healthcare_is_eur(self):
        s = self.fn("Healthcare", "Germany")
        assert s is not None
        assert s["currency"] == "EUR"
        assert s["local_display"].startswith("€")

    def test_us_healthcare_has_no_redundant_usd_equiv(self):
        s = self.fn("Healthcare", "United States")
        assert s is not None
        assert s["currency"] == "USD"
        assert "usd_display" not in s  # already USD; no redundant equivalent

    def test_japan_excludes_monthly_entries(self):
        # JP annual range must not be polluted by the ¥306,900/mo base entry
        s = self.fn("Healthcare", "Japan")
        assert s is not None
        assert s["currency"] == "JPY"
        assert s["low"] >= 1_000_000  # annual figures only

    def test_unknown_country_returns_none(self):
        assert self.fn("Healthcare", "Atlantis") is None

    def test_unknown_industry_returns_none(self):
        assert self.fn("Astrology", "United Kingdom") is None
