"""Exhaustive currency-formatting regression tests.

Pins down which currency symbol Nova will render for each country it knows
about. The chatbot is shipped to clients in 38+ markets, so a regression here
(e.g. UK queries rendering with `$` instead of `£`) is visible to every
non-US user on the first response.

Test groups:
  TestCurrencyCodeLookup     -- _get_currency_for_country mapping (40+ cases)
  TestCurrencySymbolLookup   -- _currency_symbol mapping (35+ cases)
  TestCurrencyCodeRoundtrip  -- detect country -> currency -> symbol chain
  TestCurrencyEdgeCases      -- None, empty, unknown, case-sensitivity
  TestCurrencyConsistency    -- every code in _COUNTRY_CURRENCY has a symbol

Regression coverage targets: audit T2-2, T2-5, T2-6.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

for _name in (
    "anthropic",
    "openai",
    "supabase",
    "redis",
    "qdrant_client",
    "sentence_transformers",
    "posthog",
):
    if _name not in sys.modules:
        sys.modules[_name] = types.ModuleType(_name)

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from nova import (  # noqa: E402
    _COUNTRY_CURRENCY,
    _currency_symbol,
    _detect_country,
    _get_currency_for_country,
)

# _CURRENCY_SYMBOLS lives at module scope but is private. Importing via
# `getattr` keeps this test resilient if it's ever moved.
import nova as _nova_module  # noqa: E402

_CURRENCY_SYMBOLS = getattr(_nova_module, "_CURRENCY_SYMBOLS", {})


# ---------------------------------------------------------------------------
# TestCurrencyCodeLookup -- canonical country -> ISO 4217 code mapping.
# ---------------------------------------------------------------------------


class TestCurrencyCodeLookup:
    """Regression for audit T2-5: _get_currency_for_country returns the
    correct ISO 4217 code for every country Nova supports. If a code flips
    (e.g. UK -> USD by mistake) every UK response shows the wrong currency."""

    @pytest.mark.parametrize(
        "country,expected_code",
        [
            # Major markets
            ("United Kingdom", "GBP"),
            ("India", "INR"),
            ("Germany", "EUR"),
            ("France", "EUR"),
            ("Italy", "EUR"),
            ("Spain", "EUR"),
            ("Netherlands", "EUR"),
            ("Belgium", "EUR"),
            ("Austria", "EUR"),
            ("Ireland", "EUR"),
            ("Portugal", "EUR"),
            ("Japan", "JPY"),
            ("China", "CNY"),
            ("South Korea", "KRW"),
            ("Brazil", "BRL"),
            ("Mexico", "MXN"),
            ("Canada", "CAD"),
            ("Australia", "AUD"),
            ("New Zealand", "NZD"),
            # Nordics
            ("Switzerland", "CHF"),
            ("Sweden", "SEK"),
            ("Norway", "NOK"),
            ("Denmark", "DKK"),
            # Eastern Europe
            ("Poland", "PLN"),
            ("Czech Republic", "CZK"),
            ("Hungary", "HUF"),
            ("Romania", "RON"),
            ("Turkey", "TRY"),
            # Africa
            ("South Africa", "ZAR"),
            ("Nigeria", "NGN"),
            ("Kenya", "KES"),
            ("Egypt", "EGP"),
            # Middle East
            ("Israel", "ILS"),
            ("United Arab Emirates", "AED"),
            ("Saudi Arabia", "SAR"),
            # APAC
            ("Singapore", "SGD"),
            ("Malaysia", "MYR"),
            ("Thailand", "THB"),
            ("Indonesia", "IDR"),
            ("Philippines", "PHP"),
            ("Vietnam", "VND"),
            ("Taiwan", "TWD"),
            # LATAM
            ("Colombia", "COP"),
            ("Chile", "CLP"),
            ("Argentina", "ARS"),
        ],
    )
    def test_country_returns_correct_iso_code(self, country, expected_code):
        assert _get_currency_for_country(country) == expected_code, (
            f"Currency code for {country!r} regressed -- expected "
            f"{expected_code!r}, got {_get_currency_for_country(country)!r}"
        )


# ---------------------------------------------------------------------------
# TestCurrencySymbolLookup -- ISO code -> display symbol mapping.
# ---------------------------------------------------------------------------


class TestCurrencySymbolLookup:
    """Regression for audit T2-2: _currency_symbol returns the correct
    display character for every supported ISO code. The display symbol is
    what end users see -- a wrong one looks unprofessional even if the
    underlying math is right."""

    @pytest.mark.parametrize(
        "code,expected_symbol",
        [
            ("USD", "$"),
            ("GBP", "£"),
            ("EUR", "€"),
            ("INR", "₹"),
            ("JPY", "¥"),
            ("CNY", "¥"),
            ("KRW", "₩"),
            ("BRL", "R$"),
            ("MXN", "MX$"),
            ("CAD", "C$"),
            ("AUD", "A$"),
            ("NZD", "NZ$"),
            ("CHF", "CHF"),
            ("SEK", "kr"),
            ("NOK", "kr"),
            ("DKK", "kr"),
            ("PLN", "zł"),
            ("CZK", "Kč"),
            ("HUF", "Ft"),
            ("RON", "lei"),
            ("TRY", "₺"),
            ("ZAR", "R"),
            ("NGN", "₦"),
            ("KES", "KSh"),
            ("EGP", "E£"),
            ("ILS", "₪"),
            ("AED", "AED"),
            ("SAR", "SR"),
            ("SGD", "S$"),
            ("MYR", "RM"),
            ("THB", "฿"),
            ("IDR", "Rp"),
            ("PHP", "₱"),
            ("VND", "₫"),
            ("TWD", "NT$"),
            ("COP", "COL$"),
            ("CLP", "CLP$"),
            ("ARS", "AR$"),
        ],
    )
    def test_iso_code_returns_correct_symbol(self, code, expected_symbol):
        assert _currency_symbol(code) == expected_symbol, (
            f"Display symbol for {code!r} regressed -- expected "
            f"{expected_symbol!r}, got {_currency_symbol(code)!r}"
        )


# ---------------------------------------------------------------------------
# TestCurrencyCodeRoundtrip -- "detect country from text -> get currency
# -> get symbol" must yield consistent results.
# ---------------------------------------------------------------------------


class TestCurrencyCodeRoundtrip:
    """End-to-end pipeline: query text -> _detect_country -> currency code
    -> display symbol. This is what every code path that renders money for
    an international user actually does."""

    @pytest.mark.parametrize(
        "query,expected_symbol",
        [
            # UK
            ("cpa for nurses in the UK", "£"),
            ("budget for hiring in Britain", "£"),
            ("cph in England", "£"),
            # India
            ("cpa for tech in India", "₹"),
            # Germany
            ("cpc for engineers in Germany", "€"),
            ("budget for hiring in Deutschland", "€"),
            # Other EUR
            ("cpa for nurses in France", "€"),
            ("budget in Spain", "€"),
            ("hiring in Italy", "€"),
            ("budget in Netherlands", "€"),
            # Brazil
            ("cpa in Brazil", "R$"),
            # Mexico
            ("cpa in Mexico", "MX$"),
            # Canada
            ("budget in Canada", "C$"),
            # Australia
            ("cpa in Australia", "A$"),
            # Japan
            ("hiring in Japan", "¥"),
            # USA fallback
            ("cpa for nurses in the US", "$"),
            ("budget for USA hiring", "$"),
            # No country -> defaults to $
            ("cpa for nursing", "$"),
        ],
    )
    def test_roundtrip_query_to_symbol(self, query, expected_symbol):
        country = _detect_country(query)
        code = _get_currency_for_country(country)
        symbol = _currency_symbol(code)
        assert symbol == expected_symbol, (
            f"Currency roundtrip for {query!r}: detected {country!r} "
            f"-> code {code!r} -> symbol {symbol!r} (expected "
            f"{expected_symbol!r})"
        )


# ---------------------------------------------------------------------------
# TestCurrencyEdgeCases -- None / empty / unknown / case-insensitivity.
# ---------------------------------------------------------------------------


class TestCurrencyEdgeCases:
    """Regression for the defensive fallback behaviour: bad inputs must
    return USD ('$'), never raise."""

    @pytest.mark.parametrize(
        "input_country",
        [None, "", "Atlantis", "ZZZ", "Not A Country", "   ", "foobar"],
    )
    def test_unknown_country_returns_usd(self, input_country):
        assert _get_currency_for_country(input_country) == "USD"

    @pytest.mark.parametrize(
        "input_code",
        [None, "", "XYZ", "ABCDEF", "NOT_A_CODE", "   "],
    )
    def test_unknown_code_returns_dollar(self, input_code):
        assert _currency_symbol(input_code) == "$"

    @pytest.mark.parametrize(
        "code,expected",
        [
            ("usd", "$"),
            ("USD", "$"),
            ("UsD", "$"),
            ("gbp", "£"),
            ("GBP", "£"),
            ("GbP", "£"),
            ("eur", "€"),
            ("EUR", "€"),
            ("inr", "₹"),
            ("INR", "₹"),
        ],
    )
    def test_currency_symbol_case_insensitive(self, code, expected):
        """Regression: ISO codes must work regardless of letter case --
        callers pass them through from JSON files of varying conventions."""
        assert _currency_symbol(code) == expected


# ---------------------------------------------------------------------------
# TestCurrencyConsistency -- every code declared in _COUNTRY_CURRENCY must
# have a matching symbol in _CURRENCY_SYMBOLS. Otherwise display silently
# falls back to '$' for a known country -- worse than failing loudly.
# ---------------------------------------------------------------------------


class TestCurrencyConsistency:
    """Regression for audit T2-6: prevents a country -> code mapping from
    being added without also adding a code -> symbol entry. Without this,
    a UK alias addition could ship with $ instead of £."""

    def test_every_country_currency_has_a_symbol(self):
        """Every ISO code in _COUNTRY_CURRENCY must have a symbol entry."""
        if not _CURRENCY_SYMBOLS:
            pytest.skip("_CURRENCY_SYMBOLS not exposed -- skipping consistency")
        for country, code in _COUNTRY_CURRENCY.items():
            assert code in _CURRENCY_SYMBOLS, (
                f"Country {country!r} maps to code {code!r} but no display "
                f"symbol is defined in _CURRENCY_SYMBOLS"
            )

    def test_us_is_intentionally_absent_from_country_currency(self):
        """Regression: 'United States' is intentionally OMITTED from
        _COUNTRY_CURRENCY so the default-USD path is exercised. If anyone
        adds it the default fallback becomes dead code."""
        assert "United States" not in _COUNTRY_CURRENCY

    def test_no_country_code_collisions_break_symbol_lookup(self):
        """Several countries map to EUR (France, Germany, Italy, ...).
        All of them must resolve to '€' -- not random other symbols."""
        eur_countries = [
            country for country, code in _COUNTRY_CURRENCY.items() if code == "EUR"
        ]
        assert len(eur_countries) >= 5, "Expected several EUR countries"
        for country in eur_countries:
            symbol = _currency_symbol(_get_currency_for_country(country))
            assert symbol == "€", f"EUR country {country!r} did not resolve to '€'"

    def test_currency_map_has_minimum_country_count(self):
        """Regression: the currency map must cover at least 38 countries
        (matches international_benchmarks_2026.json)."""
        assert len(_COUNTRY_CURRENCY) >= 38, (
            f"Currency map shrank below 38 countries: "
            f"{len(_COUNTRY_CURRENCY)} entries"
        )


# ---------------------------------------------------------------------------
# TestCurrencySymbolFormatting -- verifies the symbol strings render correctly
# in common money formats. Catches Unicode regressions, e.g. someone replacing
# '£' with 'GBP' would break this.
# ---------------------------------------------------------------------------


class TestCurrencySymbolFormatting:
    """Defensive checks on the byte-level shape of returned symbols."""

    def test_pound_symbol_is_actual_pound(self):
        sym = _currency_symbol("GBP")
        assert sym == "£"  # U+00A3 POUND SIGN
        assert len(sym) == 1

    def test_euro_symbol_is_actual_euro(self):
        sym = _currency_symbol("EUR")
        assert sym == "€"  # U+20AC EURO SIGN
        assert len(sym) == 1

    def test_rupee_symbol_is_actual_rupee(self):
        sym = _currency_symbol("INR")
        assert sym == "₹"  # U+20B9 INDIAN RUPEE SIGN
        assert len(sym) == 1

    def test_yen_symbol_is_actual_yen(self):
        sym = _currency_symbol("JPY")
        assert sym == "¥"  # U+00A5 YEN SIGN
        assert len(sym) == 1

    def test_dollar_symbol_is_ascii_dollar(self):
        sym = _currency_symbol("USD")
        assert sym == "$"
        assert sym == chr(0x24)
        assert len(sym) == 1

    @pytest.mark.parametrize(
        "code",
        ["BRL", "MXN", "CAD", "AUD", "NZD", "SGD", "TWD", "ARS", "COP", "CLP"],
    )
    def test_compound_dollar_symbols_contain_dollar(self, code):
        """Real currencies whose display includes '$' (R$, MX$, A$, ...)
        must keep the $ component."""
        sym = _currency_symbol(code)
        assert "$" in sym, f"Compound dollar lost from {code} symbol {sym!r}"

    def test_yen_and_yuan_share_symbol(self):
        """JPY and CNY both display as '¥' -- intentional convention."""
        assert _currency_symbol("JPY") == _currency_symbol("CNY") == "¥"


# ---------------------------------------------------------------------------
# TestCurrencyWithBenchmarkPath -- when a non-US country reaches the
# benchmark fast path, that path defers to the LLM. Verify currency-bearing
# countries actually take the defer route (not the dollar-rendering route).
# ---------------------------------------------------------------------------


class TestCurrencyWithBenchmarkPath:
    """End-to-end: verify the benchmark fast path *defers* for queries that
    would otherwise render USD when the user wants local currency."""

    # Build a fresh Nova once per class (no IO).
    @pytest.fixture
    def nova(self):
        from nova import Nova

        return Nova.__new__(Nova)

    @pytest.mark.parametrize(
        "msg",
        [
            "cpa for nurses in the UK",  # would have been £
            "cpa for tech in India",  # would have been ₹
            "cpc for engineers in Germany",  # would have been €
            "cph in Brazil",  # would have been R$
            "average cpa in Mexico",  # would have been MX$
            "cpa for nurses in Canada",  # would have been C$
            "cpc in Australia",  # would have been A$
            "cpc for tech in Japan",  # would have been ¥
        ],
    )
    def test_non_usd_country_defers_so_currency_can_be_local(self, nova, msg):
        """Regression for T2-1..T2-3: the US-only benchmark fast path MUST
        defer for any non-USD country so the LLM slow path can render local
        currency from international_benchmarks_2026.json. Without this
        defer the user gets USD numbers and the wrong symbol."""
        out = nova._fast_path_benchmark_lookup(msg, msg.lower())
        assert out is None, (
            f"Fast path should defer for non-US country query {msg!r} "
            f"(otherwise USD is rendered instead of local currency); "
            f"got fast_path response"
        )
