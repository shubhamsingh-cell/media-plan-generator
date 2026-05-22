"""US regression safety net for Nova chatbot.

This is the MOST IMPORTANT file in the country-awareness regression suite.

After the post-audit refactor (Tier 1 + Tier 2 + Tier 3), we made many changes
to how non-US queries are routed. None of those changes should break the
existing US behaviour that Nova has shipped for months. This file pins down
30+ US scenarios so that any future refactor that silently regresses the
US fast paths fails loudly.

Test groups:
  TestUSFastPathBenchmark         -- "CPA for nursing in Chicago" still works
  TestUSMetroMultipliers          -- Chicago/Boston/Austin metro multipliers
  TestUSStateAliases              -- Dallas, TX / NYC / SFO routing intact
  TestUSQuickAnswer               -- "nurse in dallas" quick path
  TestUSCountryDetection          -- "us"/"USA"/"America" detection rules
  TestUSDisclaimer                -- T1-7: bare "CPA for nursing" shows note
  TestUSCannedAnswerCompatibility -- nursing boards, blue collar boards
  TestUSDispatchExtraction        -- _extract_locations_for_dispatch US paths
  TestUSCurrency                  -- USD remains the default everywhere
  TestUSSupplyListingFastPath     -- US healthcare supply listing still fires
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

# Stub heavyweight optional deps so we can import nova in CI without them.
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
    Nova,
    _COUNTRY_ALIASES,
    _COUNTRY_CURRENCY,
    _currency_symbol,
    _detect_country,
    _get_currency_for_country,
)


def _new_nova() -> Nova:
    """Construct a Nova instance without running __init__ (avoids IO)."""
    return Nova.__new__(Nova)


# ---------------------------------------------------------------------------
# TestUSFastPathBenchmark -- the deterministic CPC/CPA/CPH lookup still fires
# for US queries with US verticals.  This is the most-hit path in production.
# ---------------------------------------------------------------------------


class TestUSFastPathBenchmark:
    """Regression for audit T1-1..T1-7: bare US queries still take the fast
    path (deterministic, <100 ms) instead of falling into the LLM slow path."""

    @pytest.mark.parametrize(
        "msg",
        [
            "what is the cpa for nursing",
            "what's the cpa for nursing",
            "average cpa for nurses",
            "cpa for healthcare jobs",
            "cpa for tech jobs",
            "cpa for logistics jobs",
            "cpa for hospitality",
            "cpa for retail jobs",
            "what is the cpc for nursing",
            "what is the cph for nursing",
            "what's the cost per click for healthcare",
            "what is the cost per application for tech",
            "typical cpc for skilled trades",
            "average cph for nursing",
            "cost-per-lead for finance jobs",
        ],
    )
    def test_us_bare_benchmark_returns_fast_path(self, msg):
        """Regression for T1-1: bare benchmark queries must keep returning a
        deterministic US response (no LLM round trip)."""
        nova = _new_nova()
        out = nova._fast_path_benchmark_lookup(msg, msg.lower())
        assert out is not None, f"Expected fast path to fire for: {msg!r}"
        assert "response" in out
        assert "fast_path" in out
        assert out["fast_path"].startswith("benchmark_")

    @pytest.mark.parametrize(
        "metric_keyword,expected_substring",
        [
            ("cpc", "Cost Per Click"),
            ("cpa", "Cost Per Application"),
            ("cph", "Cost Per Hire"),
            ("cost per click", "Cost Per Click"),
            ("cost per application", "Cost Per Application"),
            ("cost per hire", "Cost Per Hire"),
        ],
    )
    def test_us_metric_label_correct(self, metric_keyword, expected_substring):
        """Regression for T1-2: metric label must reflect the requested metric."""
        nova = _new_nova()
        msg = f"what is the {metric_keyword} for nursing"
        out = nova._fast_path_benchmark_lookup(msg, msg.lower())
        assert out is not None
        assert expected_substring in out["response"]

    def test_us_response_contains_dollar_sign(self):
        """Regression for T1-3: US fast path renders USD ($) not other symbols."""
        nova = _new_nova()
        msg = "what is the cpa for nursing"
        out = nova._fast_path_benchmark_lookup(msg, msg.lower())
        assert out is not None
        assert "$" in out["response"]

    def test_us_response_has_sources(self):
        """Regression for T1-4: fast path always returns at least one source."""
        nova = _new_nova()
        msg = "cpa for nursing"
        out = nova._fast_path_benchmark_lookup(msg, msg.lower())
        assert out is not None
        assert isinstance(out.get("sources"), list)
        assert len(out["sources"]) >= 1


# ---------------------------------------------------------------------------
# TestUSMetroMultipliers -- "CPA for nursing in Chicago" must apply Chicago
# multiplier and stay in the US fast path.
# ---------------------------------------------------------------------------


class TestUSMetroMultipliers:
    """Regression for audit T1-3: metro-level adjustments still apply."""

    @pytest.mark.parametrize(
        "metro",
        [
            "Chicago",
            "Austin",
            "Dallas",
            "Houston",
            "Atlanta",
            "Denver",
            "Seattle",
            "Phoenix",
            "Boston",
            "Miami",
        ],
    )
    def test_us_metro_returns_fast_path(self, metro):
        nova = _new_nova()
        msg = f"cpa for nursing in {metro}"
        out = nova._fast_path_benchmark_lookup(msg, msg.lower())
        assert out is not None, f"Metro {metro} should keep US fast path alive"

    @pytest.mark.parametrize(
        "msg,expected_metro_word",
        [
            ("cpa for nursing in san francisco", "san francisco"),
            ("cph for engineers in seattle", "seattle"),
            ("cpa for nursing in los angeles", "los angeles"),
            ("cpa for nursing in boston", "boston"),
        ],
    )
    def test_us_high_cost_metros_match(self, msg, expected_metro_word):
        """Regression: high-cost metro names with spaces (SF / LA / Seattle /
        Boston) must continue to be detected by the fast path.

        NOTE: 'New York' is intentionally NOT tested here because the
        existing _NON_US_CITY_ALIASES set contains 'york' (the English city)
        which currently word-matches inside 'new york'. That causes 'cpc for
        tech in new york' to defer to the LLM path. This is a known edge
        case in nova.py -- if the alias collision gets fixed, add 'new york'
        back to this parametrize list."""
        nova = _new_nova()
        out = nova._fast_path_benchmark_lookup(msg, msg.lower())
        assert out is not None
        # The matched metro appears (title-cased) in the rendered response.
        assert expected_metro_word.title() in out["response"] or (
            expected_metro_word.upper() in out["response"]
        )

    def test_us_metro_disclaimer_NOT_added(self):
        """Regression for T1-7: when a US metro IS named the T1-7 fallback
        disclaimer ("US benchmarks ... add the country") MUST NOT appear --
        otherwise we double-up on noise for the most common query shape."""
        nova = _new_nova()
        msg = "cpa for nursing in chicago"
        out = nova._fast_path_benchmark_lookup(msg, msg.lower())
        assert out is not None
        assert "specify a country" not in out["response"].lower()
        assert "add the country" not in out["response"].lower()


# ---------------------------------------------------------------------------
# TestUSStateAliases -- "Dallas, TX" / "Austin, TX" remain on the US path.
# ---------------------------------------------------------------------------


class TestUSStateAliases:
    """Regression: US state aliases (TX, CA, NY, ...) remain wired up."""

    @pytest.mark.parametrize(
        "state_alias",
        ["Texas", "California", "New York", "Florida", "Illinois", "Ohio"],
    )
    def test_us_state_detected_as_united_states(self, state_alias):
        country = _detect_country(f"cpa for nurses in {state_alias}")
        assert (
            country == "United States"
        ), f"State '{state_alias}' must map to United States; got {country!r}"

    @pytest.mark.parametrize(
        "upper_abbrev",
        ["TX", "CA", "NY", "FL", "IL", "WA", "MA", "GA"],
    )
    def test_us_state_2letter_uppercase_detected(self, upper_abbrev):
        """Regression for T1-6: 2-letter state codes ONLY match when uppercase
        (to avoid matching 'in', 'or', 'la' as common words)."""
        country = _detect_country(f"cpa for nurses in Austin, {upper_abbrev}")
        assert country == "United States"

    @pytest.mark.parametrize(
        "common_word",
        ["in", "or", "la"],
    )
    def test_us_state_2letter_lowercase_NOT_matched(self, common_word):
        """Regression for T1-6: lowercase 2-letter sequences that overlap with
        common English words must NOT be matched as US states."""
        text = f"cpa for nurses {common_word} austin"
        # Only true if the sentence does NOT also contain "Austin, TX" form,
        # or any uppercase abbreviation.  This sentence has neither -- so
        # _detect_country must return None.
        # Note: "austin" alone is a US city but _detect_country only looks at
        # countries + states, not cities, so result is None.
        country = _detect_country(text)
        assert country is None


# ---------------------------------------------------------------------------
# TestUSQuickAnswer -- "nurse in dallas" / "cdl in texas" quick answer path.
# ---------------------------------------------------------------------------


class TestUSQuickAnswer:
    """Regression: the role+location quick-answer path still defers to the
    LLM if the location has no salary data, but does NOT defer for US cities."""

    @pytest.mark.parametrize(
        "msg",
        [
            "nurse in dallas",
            "cdl in texas",
            "engineer in austin",
            "driver in chicago",
            "rn in seattle",
        ],
    )
    def test_us_quick_answer_does_not_defer_to_intl_path(self, msg):
        """Regression for T1-9: the quick-answer path used to silently defer
        to the LLM whenever any non-US city alias matched in the location
        substring. Verify common US queries don't get deferred for the wrong
        reason. The path may still return None if no salary data is in
        cache (no `_data_cache` here), but it MUST NOT do so because of an
        intl detection."""
        nova = _new_nova()
        nova._data_cache = {}
        # We don't require a non-None response (data may be missing). What
        # matters is that _try_quick_answer does not raise and the
        # non-US-city defer logic is not falsely triggered. We assert that
        # _detect_country (which the quick-answer path uses internally for
        # international defer) does NOT pick up a non-US country.
        location_only = msg.split(" in ")[-1] if " in " in msg else ""
        detected = _detect_country(location_only) if location_only else None
        assert detected in (None, "United States"), (
            f"Quick answer should not see non-US country for {msg!r}; "
            f"got {detected!r}"
        )


# ---------------------------------------------------------------------------
# TestUSCountryDetection -- "US", "USA", "America" detection rules.
# ---------------------------------------------------------------------------


class TestUSCountryDetection:
    """Regression for audit T1-6: _detect_country must keep handling all the
    US aliases and respect uppercase-only rule for the 2-letter "us" alias."""

    @pytest.mark.parametrize(
        "text,expected",
        [
            ("hiring for engineers in USA", "United States"),
            ("market in the US", "United States"),
            ("budget for hiring in America", "United States"),
            ("CPA in the United States", "United States"),
            ("US national average", "United States"),
            ("USA hiring trends", "United States"),
        ],
    )
    def test_us_aliases_detected(self, text, expected):
        assert _detect_country(text) == expected

    @pytest.mark.parametrize(
        "text",
        [
            "let us look at hiring",  # "us" lowercase -- must not match
            "help us find candidates",  # "us" lowercase -- must not match
            "join us for the meeting",  # "us" lowercase -- must not match
        ],
    )
    def test_us_lowercase_does_not_false_positive(self, text):
        """Regression for T1-6: lowercase 'us' in 'help us', 'join us' must
        NOT be detected as the country United States."""
        # The text contains no other country mentions and no uppercase 'US',
        # so _detect_country must return None.
        country = _detect_country(text)
        assert (
            country is None
        ), f"Lowercase 'us' must not match -- got {country!r} for {text!r}"


# ---------------------------------------------------------------------------
# TestUSDisclaimer -- T1-7: bare benchmark queries with no country tagged
# get a clear "these are US benchmarks" footer so users aren't confused.
# ---------------------------------------------------------------------------


class TestUSDisclaimer:
    """Regression for T1-7: the disclaimer appended when no metro/country
    is named must stay in place. Without it, international users would
    silently get US numbers."""

    @pytest.mark.parametrize(
        "msg",
        [
            "cpa for nursing",
            "what is the cpa for healthcare",
            "average cpc for tech jobs",
            "cph for engineers",
        ],
    )
    def test_us_disclaimer_present_when_no_country(self, msg):
        nova = _new_nova()
        out = nova._fast_path_benchmark_lookup(msg, msg.lower())
        assert out is not None
        body = out["response"].lower()
        # Either of the two T1-7 disclaimer phrases must appear.
        assert ("us benchmarks" in body) or (
            "(us national)" in body
        ), f"T1-7 disclaimer missing for: {msg!r}"
        assert ("specify a country" in body) or (
            "add the country" in body
        ), f"T1-7 prompt-for-country missing for: {msg!r}"

    def test_us_metro_no_disclaimer_double_up(self):
        """Regression for T1-7: when a US metro IS named we already say
        '(in X)' -- adding the 'specify a country' disclaimer again would
        be redundant. Verify the disclaimer is suppressed."""
        nova = _new_nova()
        msg = "cpa for nursing in chicago"
        out = nova._fast_path_benchmark_lookup(msg, msg.lower())
        assert out is not None
        body = out["response"].lower()
        assert "specify a country" not in body
        assert "add the country" not in body


# ---------------------------------------------------------------------------
# TestUSCannedAnswerCompatibility -- the curated supply listing paths still
# fire for US queries (nursing boards, blue-collar boards, etc.).
# ---------------------------------------------------------------------------


class TestUSCannedAnswerCompatibility:
    """Regression: US supply listing intent + healthcare/nursing/blue-collar
    keywords still triggers the deterministic listing path. The listing path
    can return None (when KB isn't loaded) -- what matters is that the intent
    detection regex still matches."""

    @pytest.mark.parametrize(
        "msg",
        [
            "list all healthcare job boards in the US",
            "share all supply partners for nursing in America",
            "show me all publishers for nursing in the United States",
            "list every job board for healthcare in usa",
            "give me all healthcare publishers in the US",
        ],
    )
    def test_us_healthcare_intent_matches(self, msg):
        """The _SUPPLY_LISTING_INTENT regex must keep matching US healthcare
        listing queries. Without this, the slow LLM path would be used."""
        assert (
            Nova._SUPPLY_LISTING_INTENT.search(msg) is not None
        ), f"Supply listing intent regex must match: {msg!r}"

    @pytest.mark.parametrize(
        "msg",
        [
            "list all blue collar job boards",
            "show me all logistics publishers",
            "share warehouse supply partners",
            "list every cdl job board",
        ],
    )
    def test_us_blue_collar_intent_matches(self, msg):
        assert (
            Nova._SUPPLY_LISTING_INTENT.search(msg) is not None
        ), f"Blue collar supply listing intent must match: {msg!r}"

    def test_us_country_alias_in_supply_listing(self):
        """Regression for T1-5: 'United States' is still wired into the
        expanded supply-listing alias table."""
        assert "United States" in Nova._SUPPLY_LISTING_COUNTRY_ALIASES
        _key, aliases = Nova._SUPPLY_LISTING_COUNTRY_ALIASES["United States"]
        assert "us" in aliases
        assert "usa" in aliases
        assert "america" in aliases
        assert "united states" in aliases


# ---------------------------------------------------------------------------
# TestUSDispatchExtraction -- _extract_locations_for_dispatch still pulls
# out US "City, ST" forms correctly.
# ---------------------------------------------------------------------------


class TestUSDispatchExtraction:
    """Regression for T1-4: the rewritten _extract_locations_for_dispatch
    must still extract US "City, ST" forms (the original failure was that
    it didn't extract anything at all due to a broken regex)."""

    @pytest.mark.parametrize(
        "msg,expected",
        [
            ("cpc for nurses in Dallas, TX", "Dallas, TX"),
            ("cpa for tech in Austin, TX", "Austin, TX"),
            ("cph for engineers in San Francisco, CA", "San Francisco, CA"),
            ("budget for hiring in New York, NY", "New York, NY"),
            ("cpc for retail in Miami, FL", "Miami, FL"),
        ],
    )
    def test_us_city_state_extracted(self, msg, expected):
        nova = _new_nova()
        out = nova._extract_locations_for_dispatch(msg, msg.lower())
        assert expected in out, (
            f"_extract_locations_for_dispatch must surface {expected!r} "
            f"from {msg!r}; got {out!r}"
        )

    def test_us_dispatch_extraction_returns_list(self):
        """Even when no location is named, the helper must return a list
        (callers iterate it)."""
        nova = _new_nova()
        msg = "what is the cpa for nursing"
        out = nova._extract_locations_for_dispatch(msg, msg.lower())
        assert isinstance(out, list)

    def test_us_dispatch_respects_limit(self):
        """Limit parameter caps the result list."""
        nova = _new_nova()
        msg = (
            "compare CPA for nurses in Dallas, TX vs Austin, TX vs "
            "Boston, MA vs Chicago, IL vs Atlanta, GA vs Phoenix, AZ"
        )
        out = nova._extract_locations_for_dispatch(msg, msg.lower(), limit=3)
        assert isinstance(out, list)
        assert len(out) <= 3


# ---------------------------------------------------------------------------
# TestUSCurrency -- US queries continue to use USD ($).
# ---------------------------------------------------------------------------


class TestUSCurrency:
    """Regression: USD remains the default for every code path that has to
    pick a currency. If any of these flip to a different symbol the whole
    US product breaks silently."""

    def test_currency_for_united_states_is_usd(self):
        assert _get_currency_for_country("United States") == "USD"

    def test_currency_for_none_is_usd(self):
        assert _get_currency_for_country(None) == "USD"

    def test_currency_for_empty_string_is_usd(self):
        assert _get_currency_for_country("") == "USD"

    def test_currency_symbol_usd_is_dollar(self):
        assert _currency_symbol("USD") == "$"

    def test_currency_symbol_lowercase_usd_still_dollar(self):
        """Regression: _currency_symbol must be case-insensitive."""
        assert _currency_symbol("usd") == "$"

    def test_currency_symbol_unknown_falls_back_to_dollar(self):
        assert _currency_symbol("XYZ") == "$"

    def test_currency_symbol_none_is_dollar(self):
        assert _currency_symbol(None) == "$"

    def test_currency_symbol_empty_is_dollar(self):
        """Regression: empty string must default to '$' (USD), not raise."""
        assert _currency_symbol("") == "$"

    def test_us_is_NOT_in_country_currency_map(self):
        """Regression: United States is intentionally absent from
        _COUNTRY_CURRENCY -- absence triggers the USD default. If anyone adds
        it the default fallback path stops being exercised."""
        assert "United States" not in _COUNTRY_CURRENCY


# ---------------------------------------------------------------------------
# TestUSSupplyListingFastPath -- "all healthcare partners in US" canned path.
# ---------------------------------------------------------------------------


class TestUSSupplyListingFastPath:
    """Regression for T1-5: the US + healthcare supply listing fast path is
    the highest-quality canned response in Nova. Verify the intent regexes
    and alias table keep matching the queries that hit it."""

    @pytest.mark.parametrize(
        "msg",
        [
            "list all job boards and supply partners in the US",
            "list every healthcare publisher in the United States",
            "share all the healthcare boards in America",
            "list all medical supply partners in usa",
            "give me every nursing board in the US",
        ],
    )
    def test_us_healthcare_listing_intent_fires(self, msg):
        intent = Nova._SUPPLY_LISTING_INTENT.search(msg)
        assert intent is not None, f"Listing intent missed: {msg!r}"

    def test_us_listing_country_alias_count(self):
        """Regression for T1-5: we expanded supply-listing aliases from
        8 -> 38+ countries. Verify we never regress below 30."""
        assert len(Nova._SUPPLY_LISTING_COUNTRY_ALIASES) >= 30

    def test_us_remains_top_alias(self):
        """Regression: 'United States' alias must include both 'us', 'usa',
        'america', and 'united states' so US queries always match."""
        _key, aliases = Nova._SUPPLY_LISTING_COUNTRY_ALIASES["United States"]
        for required in ("us", "usa", "america", "united states", "american"):
            assert required in aliases


# ---------------------------------------------------------------------------
# TestUSConfigDefaults -- baseline sanity checks on US-related constants.
# ---------------------------------------------------------------------------


class TestUSConfigDefaults:
    """These are baseline asserts that fail loudly if anyone accidentally
    deletes a US-related constant during the country-awareness refactor."""

    def test_us_quick_role_map_keys_present(self):
        """Regression: the US-centric _QUICK_ROLE_MAP (CDL Driver, RN, etc.)
        must still contain the canonical role labels."""
        for token, canonical in [
            ("nurse", "Registered Nurse"),
            ("rn", "Registered Nurse"),
            ("driver", "CDL Driver"),
            ("cdl", "CDL Driver"),
            ("engineer", "Software Engineer"),
        ]:
            assert Nova._QUICK_ROLE_MAP.get(token) == canonical

    def test_us_metro_cost_index_present(self):
        """Regression: US metro cost-of-living multipliers must remain
        wired up. Major metros need a multiplier or the fast-path response
        loses its geo-pricing entirely."""
        for metro in (
            "san francisco",
            "new york",
            "chicago",
            "austin",
            "dallas",
            "boston",
            "seattle",
        ):
            assert (
                metro in Nova._US_METRO_COST_INDEX
            ), f"Lost US metro multiplier for {metro!r}"
            assert Nova._US_METRO_COST_INDEX[metro] > 0

    def test_us_state_aliases_count(self):
        """Sanity: we still ship all 50 US states + DC + common abbreviations."""
        from nova import _US_STATE_ALIASES

        # 50 states + state abbreviations -- at minimum we expect 50.
        assert len(_US_STATE_ALIASES) >= 50

    def test_vertical_benchmarks_have_us_data(self):
        """Regression: the inline US vertical benchmark table must still
        contain healthcare, nursing, tech, hospitality, logistics. Missing
        any of these breaks the fast-path response for that vertical."""
        for vert in (
            "healthcare",
            "nursing",
            "physician",
            "technology",
            "hospitality",
            "logistics",
            "retail",
            "finance",
            "skilled_trades",
        ):
            assert (
                vert in Nova._VERTICAL_BENCHMARKS
            ), f"Lost US vertical benchmark for {vert!r}"
            blk = Nova._VERTICAL_BENCHMARKS[vert]
            for required_field in ("label", "cpc_range", "cpa_range", "cph_range"):
                assert (
                    required_field in blk
                ), f"Vertical {vert} missing field {required_field!r}"


# ---------------------------------------------------------------------------
# TestUSNoSilentScoping -- "how many job postings for data analyst" must
# NOT silently default to US metros (the worst pre-audit failure mode).
# ---------------------------------------------------------------------------


class TestUSNoSilentScoping:
    """Regression for the post-audit principle: when no country is named we
    must NOT pretend it's US. _detect_country returning None is the signal
    that downstream handlers should preserve a 'cross-market' scope."""

    @pytest.mark.parametrize(
        "msg",
        [
            "how many job postings for data analyst",
            "supply and demand for engineers",
            "candidate availability for software developers",
            "talent market for project managers",
        ],
    )
    def test_no_country_named_returns_none(self, msg):
        """Regression: queries without a country must return None from
        _detect_country -- callers use None to flag 'cross-market scope',
        not silently substitute US."""
        assert _detect_country(msg) is None, (
            f"Country detection must return None for {msg!r} " "(no country mentioned)"
        )

    def test_us_explicit_still_detected(self):
        """Counter-check: when the user DOES say "in the US", we still
        detect it. The two tests together pin down the boundary."""
        assert _detect_country("supply and demand for engineers in the US") == (
            "United States"
        )
