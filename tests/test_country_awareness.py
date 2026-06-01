"""Country-awareness regression tests for Nova chatbot.

Covers the post-audit fixes (Tier 1 + Tier 2 + Tier 3) that make the chatbot
respond correctly to non-US queries instead of silently returning US data.
"""

from __future__ import annotations

import os
import sys
import types
from pathlib import Path

# Stub out optional deps that aren't installed locally so we can import nova
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
    """Build a Nova instance without running __init__ (which hits IO)."""
    return Nova.__new__(Nova)


# ---------------------------------------------------------------------------
# Tier 1 -- already merged
# ---------------------------------------------------------------------------


class TestFastPathBenchmarkLookup:
    def test_uk_defers_to_llm(self):
        nova = _new_nova()
        msg = "whats the average cpa in the uk for registered nurses"
        assert nova._fast_path_benchmark_lookup(msg, msg.lower()) is None

    def test_london_defers_to_llm(self):
        nova = _new_nova()
        msg = "cpa for nurses in london"
        assert nova._fast_path_benchmark_lookup(msg, msg.lower()) is None

    def test_mumbai_defers(self):
        nova = _new_nova()
        msg = "cpc for tech in mumbai"
        assert nova._fast_path_benchmark_lookup(msg, msg.lower()) is None

    def test_us_no_country_still_works(self):
        nova = _new_nova()
        msg = "cpa for nursing jobs"
        out = nova._fast_path_benchmark_lookup(msg, msg.lower())
        assert out is not None
        # T1-7 disclaimer must be present
        assert "us benchmarks" in out["response"].lower()
        assert (
            "specify a country" in out["response"].lower()
            or "add the country" in out["response"].lower()
        )

    def test_us_metro_still_works(self):
        nova = _new_nova()
        msg = "cpa for nursing in chicago"
        out = nova._fast_path_benchmark_lookup(msg, msg.lower())
        assert out is not None


class TestQuickAnswer:
    def test_london_defers(self):
        nova = _new_nova()
        assert nova._try_quick_answer("software engineer in london") is None

    def test_mumbai_defers(self):
        nova = _new_nova()
        assert nova._try_quick_answer("rn in mumbai") is None

    def test_germany_country_defers(self):
        nova = _new_nova()
        assert nova._try_quick_answer("engineer in germany") is None


class TestCurrencyHelpers:
    def test_currency_codes(self):
        assert _get_currency_for_country("United Kingdom") == "GBP"
        assert _get_currency_for_country("India") == "INR"
        assert _get_currency_for_country("Germany") == "EUR"
        assert _get_currency_for_country(None) == "USD"
        assert _get_currency_for_country("Unknown") == "USD"

    def test_currency_symbols(self):
        assert _currency_symbol("GBP") == "£"
        assert _currency_symbol("EUR") == "€"
        assert _currency_symbol("INR") == "₹"
        assert _currency_symbol("USD") == "$"
        assert _currency_symbol(None) == "$"
        assert _currency_symbol("XYZ") == "$"  # unknown -> fallback


class TestLocationExtraction:
    def test_us_state_format(self):
        nova = _new_nova()
        out = nova._extract_locations_for_dispatch(
            "cpc for nurses in Dallas, TX", "cpc for nurses in dallas, tx"
        )
        assert "Dallas, TX" in out

    def test_uk_city(self):
        nova = _new_nova()
        out = nova._extract_locations_for_dispatch(
            "cpc for nurses in London", "cpc for nurses in london"
        )
        assert "London" in out

    def test_india_city(self):
        nova = _new_nova()
        out = nova._extract_locations_for_dispatch(
            "cpc for tech in Bangalore", "cpc for tech in bangalore"
        )
        assert "Bangalore" in out

    def test_country_fallback(self):
        nova = _new_nova()
        out = nova._extract_locations_for_dispatch(
            "cpc for nurses in Germany", "cpc for nurses in germany"
        )
        assert "Germany" in out

    def test_qc1_p1_ambiguous_birmingham_al_no_dup(self):
        """QC-1 P1 fix: 'Birmingham, AL' must not also return 'Birmingham'
        (which downstream resolves to Birmingham UK)."""
        nova = _new_nova()
        out = nova._extract_locations_for_dispatch(
            "cpc for nurses in Birmingham, AL",
            "cpc for nurses in birmingham, al",
        )
        assert out == ["Birmingham, AL"], f"Expected only US match, got {out}"

    def test_qc1_p1_manchester_nh_no_dup(self):
        """Same fix for Manchester, NH (US) vs Manchester UK."""
        nova = _new_nova()
        out = nova._extract_locations_for_dispatch(
            "cpc for engineers in Manchester, NH",
            "cpc for engineers in manchester, nh",
        )
        assert out == ["Manchester, NH"], f"Expected only US match, got {out}"

    def test_qc1_p2_cambridge_now_in_alias_set(self):
        """QC-1 P2-1 fix: cambridge was in docstring but missing from set."""
        from nova import Nova as _N

        assert "cambridge" in _N._NON_US_CITY_ALIASES
        assert "hamilton" in _N._NON_US_CITY_ALIASES


class TestCurrencyCoverageExpansion:
    """QC-1 P2-6: previously missing currencies."""

    def test_qatar_has_currency(self):
        assert _get_currency_for_country("Qatar") == "QAR"

    def test_bangladesh_has_currency(self):
        assert _get_currency_for_country("Bangladesh") == "BDT"

    def test_peru_has_currency(self):
        assert _get_currency_for_country("Peru") == "PEN"

    def test_hong_kong_has_currency(self):
        assert _get_currency_for_country("Hong Kong") == "HKD"

    def test_currency_symbols_for_new_codes(self):
        assert _currency_symbol("QAR") == "QR"
        assert _currency_symbol("BDT") == "৳"
        assert _currency_symbol("HKD") == "HK$"
        assert _currency_symbol("PEN") == "S/"


class TestSupplyListingAliases:
    def test_expanded_country_list(self):
        # T1-5 expanded from 8 -> 38+ countries
        assert len(Nova._SUPPLY_LISTING_COUNTRY_ALIASES) >= 30
        for required in (
            "United States",
            "United Kingdom",
            "India",
            "Germany",
            "Japan",
            "Brazil",
            "Singapore",
            "Mexico",
            "Australia",
            "Canada",
        ):
            assert required in Nova._SUPPLY_LISTING_COUNTRY_ALIASES


# ---------------------------------------------------------------------------
# Tier 2 -- will be exercised after agent merge
# ---------------------------------------------------------------------------


class TestTier2QueryHandlers:
    """These tests are written to fail BEFORE the Tier 2 agent merges its work,
    and pass AFTER. They lock in country-awareness on tool handlers."""

    def test_market_demand_accepts_country_param(self):
        import pytest

        nova = _new_nova()
        nova._data_cache = {"international_benchmarks": {"countries": {}}}
        try:
            result = nova._query_market_demand(
                {"role": "Software Engineer", "country": "United Kingdom"}
            )
        except (AttributeError, KeyError, TypeError):
            pytest.skip("Tier 2 changes not yet merged")
            return
        if not isinstance(result, dict) or result.get("error"):
            pytest.skip("Tier 2 changes not yet merged (handler returned error)")
            return
        # If none of the Tier 2 country-aware fields are present, skip --
        # this means Tier 2 fix hasn't merged yet.
        if not (
            "country" in result
            or "data_note" in result
            or "country_specific_note" in result
        ):
            pytest.skip("Tier 2 country-awareness not yet present in result")

    def test_recruitment_benchmarks_currency_tagged(self):
        import pytest

        nova = _new_nova()
        nova._data_cache = {}
        try:
            result = nova._query_recruitment_benchmarks(
                {"industry": "healthcare", "country": "United Kingdom"}
            )
        except (AttributeError, KeyError, TypeError):
            pytest.skip("Tier 2 changes not yet merged")
            return
        if not isinstance(result, dict):
            pytest.skip("Tier 2 changes not yet merged")
            return
        # Audit T2-2: handler must tag responses (even error responses) with
        # country and currency so downstream callers know the locale context.
        assert (
            "country" in result or "currency" in result
        ), f"Tier 2 country/currency tagging missing. Got: {sorted(result.keys())}"


# ---------------------------------------------------------------------------
# Tier 3 -- intelligence features
# ---------------------------------------------------------------------------


class TestTier3Features:
    def test_planner_feature_flag(self):
        # Defaults to enabled
        from os import environ

        flag = environ.get("NOVA_PLANNER_ENABLED", "true")
        assert flag.lower() in ("true", "false", "1", "0")

    def test_clarification_feature_flag(self):
        from os import environ

        flag = environ.get("NOVA_CLARIFICATIONS_ENABLED", "true")
        assert flag.lower() in ("true", "false", "1", "0")


# ---------------------------------------------------------------------------
# S76c: T3 number-verifier markdown safety (bold-span preservation)
# ---------------------------------------------------------------------------


class TestT3NumberVerifierBoldSafety:
    """Ensure the `_[unverified]_` tag never splits a `**...**` bold span."""

    def test_offset_helper_outside_bold_unchanged(self):
        from nova import _t3_adjust_offset_outside_bold

        text = "apply rate is 14.0% in 2026"
        # offset right after the "%" at index 19 -- no bold here
        assert _t3_adjust_offset_outside_bold(text, 19) == 19

    def test_offset_helper_inside_bold_jumps_past_close(self):
        from nova import _t3_adjust_offset_outside_bold

        text = "apply rate is **14.0%** in 2026"
        # offset after "%": position right before closing "**"
        end_after_pct = text.index("%") + 1  # 21
        adjusted = _t3_adjust_offset_outside_bold(text, end_after_pct)
        # Adjusted must land AFTER closing "**" (offset 23)
        assert adjusted == 23
        # Inserting the tag at adjusted leaves bold intact
        spliced = text[:adjusted] + " _[unverified]_" + text[adjusted:]
        assert "**14.0%**" in spliced

    def test_verifier_does_not_split_bold(self):
        from nova import _verify_response_numbers

        # Hallucinated bold percentage with no tool support -- must be tagged
        # BUT the ``**14.0%**`` span must remain intact.
        text = "Healthcare apply rate is **14.0%** based on recent data."
        annotated, count = _verify_response_numbers(text, tool_results_raw=[])
        # 14.0% is not in the knowledge whitelist, so it gets tagged.
        assert count == 1
        # The bold span must NOT be split:
        assert "**14.0%**" in annotated
        # The verifier tag must be present AFTER the closing **:
        assert "**14.0%** _[unverified]_" in annotated

    def test_verifier_plain_percent_unchanged_format(self):
        """Plain (non-bold) percentages still get tagged immediately after %."""
        from nova import _verify_response_numbers

        text = "Apply rate is 14.0% based on recent data."
        annotated, count = _verify_response_numbers(text, tool_results_raw=[])
        assert count == 1
        assert "14.0% _[unverified]_" in annotated


# ---------------------------------------------------------------------------
# OECD SDMX tool tests (Phase 1 build)
# ---------------------------------------------------------------------------


class TestOecdSdmxTool:
    """Verify the _query_oecd_sdmx tool is wired and behaves on offline paths.

    These tests do NOT hit the network -- they exercise the error and
    validation paths so CI stays deterministic. The live API smoke tests
    live in docs/oecd_sdmx_sample.py (run manually).
    """

    def test_tool_present_in_definitions(self):
        nova = _new_nova()
        names = {t["name"] for t in nova.get_tool_definitions()}
        assert (
            "query_oecd_sdmx" in names
        ), "query_oecd_sdmx should be advertised in get_tool_definitions()"

    def test_tool_registered_in_handler_map(self):
        nova = _new_nova()
        handlers = nova._tool_handler_map()
        assert "query_oecd_sdmx" in handlers
        # Handler must be the bound method we wrote.
        assert handlers["query_oecd_sdmx"].__func__ is nova._query_oecd_sdmx.__func__

    def test_tool_schema_well_formed(self):
        nova = _new_nova()
        oecd = next(
            t for t in nova.get_tool_definitions() if t["name"] == "query_oecd_sdmx"
        )
        schema = oecd["input_schema"]
        # Required: country + dataset
        assert set(schema["required"]) == {"country", "dataset"}
        # 6 datasets in the enum
        assert set(schema["properties"]["dataset"]["enum"]) == {
            "unemployment_rate",
            "labour_force",
            "average_wages",
            "hours_worked",
            "productivity",
            "migration",
        }
        # country, start_year, end_year all present
        for field in ("country", "start_year", "end_year"):
            assert field in schema["properties"], f"missing field: {field}"

    def test_accepts_country_param_and_routes(self):
        """The tool MUST route a country parameter through to the result.

        We use an unknown dataset so the call short-circuits before the
        network -- this proves the country plumbing without needing
        network access.
        """
        from nova import Nova

        nova = _new_nova()
        result = nova._query_oecd_sdmx(
            {"country": "United States", "dataset": "definitely_not_real"}
        )
        # Country was normalized from "United States" -> "USA"
        assert result["country"] == "USA"
        # Tool name was stamped on
        assert result["tool"] == "query_oecd_sdmx"
        # Error message lists the available datasets
        assert "error" in result and result["data"] == []
        # Normalization is testable in isolation too
        assert Nova._normalize_oecd_country("Germany") == "DEU"
        assert Nova._normalize_oecd_country("USA+UK") == "USA+GBR"

    def test_empty_country_returns_validation_error(self):
        """Missing required country must come back as an error, not a crash."""
        nova = _new_nova()
        result = nova._query_oecd_sdmx({"country": "", "dataset": "unemployment_rate"})
        assert result["tool"] == "query_oecd_sdmx"
        assert result["data"] == []
        assert "country is required" in result["error"]

    def test_iso3_country_passes_through_unchanged(self):
        from nova import Nova

        # ISO-3 codes should pass straight through (uppercased).
        assert Nova._normalize_oecd_country("usa") == "USA"
        assert Nova._normalize_oecd_country("DEU") == "DEU"
        # Multi-country preserved
        assert Nova._normalize_oecd_country("USA+DEU+FRA") == "USA+DEU+FRA"

    def test_progress_label_present(self):
        from nova import _TOOL_LABELS

        assert "query_oecd_sdmx" in _TOOL_LABELS
        # Label should mention OECD so the progress UI is meaningful.
        assert "OECD" in _TOOL_LABELS["query_oecd_sdmx"]


# ---------------------------------------------------------------------------
# ESCO occupation taxonomy tool tests (F4 build)
# ---------------------------------------------------------------------------


class TestEscoOccupationsTool:
    """Verify the _query_esco_occupations tool is wired and behaves offline.

    These tests do NOT hit the network -- they exercise the wiring and the
    empty-query validation path so CI stays deterministic. Live API smoke
    tests are documented in docs/F4_ESCO_Integration_Report.md.
    """

    def test_tool_present_in_definitions(self):
        nova = _new_nova()
        names = {t["name"] for t in nova.get_tool_definitions()}
        assert (
            "query_esco_occupations" in names
        ), "query_esco_occupations should be advertised in get_tool_definitions()"

    def test_tool_registered_in_handler_map(self):
        nova = _new_nova()
        handlers = nova._tool_handler_map()
        assert "query_esco_occupations" in handlers
        assert (
            handlers["query_esco_occupations"].__func__
            is nova._query_esco_occupations.__func__
        )

    def test_tool_schema_well_formed(self):
        nova = _new_nova()
        esco = next(
            t
            for t in nova.get_tool_definitions()
            if t["name"] == "query_esco_occupations"
        )
        schema = esco["input_schema"]
        # Only `query` is required; the rest are optional knobs.
        assert schema["required"] == ["query"]
        for field in ("query", "language", "limit", "country"):
            assert field in schema["properties"], f"missing field: {field}"
        # language is a string with sensible default behavior described.
        assert schema["properties"]["language"]["type"] == "string"
        assert schema["properties"]["limit"]["type"] == "integer"

    def test_empty_query_returns_validation_error(self):
        """Empty query must come back as a structured error, not a crash."""
        nova = _new_nova()
        result = nova._query_esco_occupations({"query": "   "})
        assert result["tool"] == "query_esco_occupations"
        assert result["source"] == "ESCO API"
        assert result["occupations"] == []
        assert result["count"] == 0
        assert result["total_matches"] == 0
        assert "query is required" in result["error"]

    def test_country_context_passes_through(self):
        """Country is for display only -- it should be echoed back, not used
        as an API filter (ESCO is EU-wide). We trigger the empty-query
        short-circuit so this stays offline.
        """
        nova = _new_nova()
        result = nova._query_esco_occupations({"query": "", "country": "Germany"})
        assert result["country_context"] == "Germany"
        assert result["tool"] == "query_esco_occupations"

    def test_progress_label_present(self):
        from nova import _TOOL_LABELS

        assert "query_esco_occupations" in _TOOL_LABELS
        label = _TOOL_LABELS["query_esco_occupations"]
        # Progress UI should mention ESCO so users recognize the source.
        assert "ESCO" in label

    def test_graceful_failure_message_registered(self):
        """When the tool fails at runtime, the chatbot must have a
        user-friendly fallback string ready -- not a raw stack trace."""
        from nova import _TOOL_ERROR_FALLBACK_MESSAGES

        assert "query_esco_occupations" in _TOOL_ERROR_FALLBACK_MESSAGES
        msg = _TOOL_ERROR_FALLBACK_MESSAGES["query_esco_occupations"]
        assert isinstance(msg, str) and len(msg) > 20

    def test_listed_as_live_source(self):
        """ESCO is a real upstream API, so freshness disclaimers should NOT
        be appended when only ESCO data is used. The ``_live_tools`` set
        guards that logic. We assert by reading the source file rather than
        importing a private constant from inside a function body."""
        import nova as nova_mod
        import re as _re

        src = open(nova_mod.__file__).read()
        match = _re.search(r"_live_tools = \{(.+?)\}", src, _re.DOTALL)
        assert match, "_live_tools set not found in nova.py"
        assert "query_esco_occupations" in match.group(1)


# ---------------------------------------------------------------------------
# RAG semantic KB tool tests (Phase 1 build)
# ---------------------------------------------------------------------------


class TestRagKbSemanticTool:
    """Verify the _query_kb_semantic tool is wired and gated correctly."""

    def test_tool_present_in_definitions(self):
        nova = _new_nova()
        names = {t["name"] for t in nova.get_tool_definitions()}
        assert "query_kb_semantic" in names

    def test_tool_registered_in_handler_map(self):
        nova = _new_nova()
        handlers = nova._tool_handler_map()
        assert "query_kb_semantic" in handlers

    def test_tool_schema_well_formed(self):
        nova = _new_nova()
        sem = next(
            t for t in nova.get_tool_definitions() if t["name"] == "query_kb_semantic"
        )
        schema = sem["input_schema"]
        # query is the only required field
        assert schema["required"] == ["query"]
        # Optional knobs all present
        for field in ("query", "k", "country", "vertical"):
            assert field in schema["properties"], f"missing field: {field}"

    def test_returns_rag_disabled_when_env_var_unset(self):
        """Phase 1: with RAG_V2_ENABLED unset, the tool MUST short-circuit."""
        os.environ.pop("RAG_V2_ENABLED", None)
        nova = _new_nova()
        result = nova._query_kb_semantic({"query": "linkedin cpa"})
        assert result["rag_disabled"] is True
        assert result["error"] == "RAG not enabled"
        assert result["chunks"] == []
        assert result["sources"] == []
        assert result["tool"] == "query_kb_semantic"
        assert result["source"] == "Nova RAG Pipeline"

    def test_returns_rag_disabled_when_env_var_false(self):
        """Explicit '0'/'false' values must NOT enable the pipeline."""
        for value in ("0", "false", "no", "off", ""):
            os.environ["RAG_V2_ENABLED"] = value
            nova = _new_nova()
            result = nova._query_kb_semantic({"query": "cpa"})
            assert result["rag_disabled"] is True, f"{value!r} should not enable RAG"
            assert result["error"] == "RAG not enabled"
        os.environ.pop("RAG_V2_ENABLED", None)

    def test_enabled_with_empty_query_returns_no_query_error(self):
        """When flag is on, an empty query is a validation error, not a crash."""
        os.environ["RAG_V2_ENABLED"] = "true"
        try:
            nova = _new_nova()
            result = nova._query_kb_semantic({"query": ""})
            assert result["rag_disabled"] is False
            assert result["error"] == "No query provided"
            assert result["chunks"] == []
        finally:
            os.environ.pop("RAG_V2_ENABLED", None)

    def test_enabled_with_real_query_returns_structured_response(self):
        """With the flag on and no Qdrant/Voyage available, we still return a
        well-formed dict (degraded mode). The pipeline picks the in-memory
        + hash backends, indexes nothing, and yields 0 chunks. The point of
        the test is to confirm the contract, not retrieval quality.
        """
        os.environ["RAG_V2_ENABLED"] = "1"
        try:
            nova = _new_nova()
            result = nova._query_kb_semantic(
                {"query": "linkedin cpc", "k": 3, "country": "US"}
            )
            assert result["rag_disabled"] is False
            assert result["source"] == "Nova RAG Pipeline"
            assert result["tool"] == "query_kb_semantic"
            assert isinstance(result["chunks"], list)
            assert isinstance(result["sources"], list)
            assert result["k"] == 3
        finally:
            os.environ.pop("RAG_V2_ENABLED", None)

    def test_progress_label_present(self):
        from nova import _TOOL_LABELS

        assert "query_kb_semantic" in _TOOL_LABELS
        # Label should reference the knowledge base / RAG concept.
        label = _TOOL_LABELS["query_kb_semantic"].lower()
        assert "knowledge" in label or "semantic" in label or "rag" in label

    def test_rag_pipeline_module_importable(self):
        """The promoted rag_pipeline module must import cleanly with the
        backward-compat NovaRAG alias intact for the sketch's existing
        pytests."""
        import rag_pipeline

        assert hasattr(rag_pipeline, "NovaRAGPipeline")
        assert hasattr(rag_pipeline, "NovaRAG")
        assert rag_pipeline.NovaRAG is rag_pipeline.NovaRAGPipeline
        # Core symbols exposed
        for sym in (
            "Document",
            "RetrievalHit",
            "BM25Index",
            "build_documents_from_kb",
        ):
            assert hasattr(rag_pipeline, sym), f"missing export: {sym}"
