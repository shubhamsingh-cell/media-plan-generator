"""Tests for the S89 PPTX deck polish (ppt_generator).

Covers the four executive-grade polish items, all offline (no network / no LLM
/ no Supabase -- the budget engine runs purely on its deterministic math):

  1. Currency-correctness -- money renders in the plan's own symbol (USD default,
     GBP / EUR / INR for non-US plans) via plan_currency wiring.
  2. Brand chart fonts -- bundled Poppins .ttf register with matplotlib, with a
     graceful DejaVu fallback when the fonts are absent.
  3. Data-sources slide freshness ("as of <date>") + provenance footer +
     "Joveo measured" callout (only when real_outcomes is present; absent-safe).
  4. Channel table has a totals row.

Plus: the deck still builds end-to-end from a minimal dict.

Runs under pytest, or standalone: ``python3 tests/test_ppt_polish.py``.
"""

from __future__ import annotations

import datetime
import io
import sys
from pathlib import Path
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import ppt_generator as ppt  # noqa: E402
from pptx import Presentation  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _build_alloc(country: str = "United States") -> dict:
    """Drive the real budget engine to produce an authentic _budget_allocation."""
    import budget_engine

    return budget_engine.calculate_budget_allocation(
        total_budget=150_000,
        roles=[
            {"title": "Registered Nurse", "count": 40, "tier": "mid"},
            {"title": "ICU Nurse", "count": 15, "tier": "senior"},
        ],
        locations=[{"city": "Metro", "state": "", "country": country}],
        industry="healthcare",
        channel_percentages={"Indeed": 40, "LinkedIn": 35, "Google Search Ads": 25},
        collar_type="white",
        campaign_start_month=9,
    )


def _plan(country: str = "United States") -> dict:
    return {
        "client_name": "Mercy Health",
        "industry": "healthcare",
        "budget": "$150,000",
        "locations": [country],
        "roles": ["Registered Nurse", "ICU Nurse"],
        "target_roles": [
            {"title": "Registered Nurse"},
            {"title": "ICU Nurse"},
        ],
        "_budget_allocation": _build_alloc(country),
    }


def _all_text(pptx_bytes: bytes) -> list[str]:
    prs = Presentation(io.BytesIO(pptx_bytes))
    out: list[str] = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if shape.has_text_frame:
                out.append(shape.text_frame.text)
    return out


def _data_sources_text(data: dict) -> list[str]:
    """Render just the data-sources slide and return its text fragments."""
    prs = Presentation()
    prs.slide_width = ppt.SLIDE_WIDTH
    prs.slide_height = ppt.SLIDE_HEIGHT
    ppt._build_slide_data_sources(prs, data)
    out: list[str] = []
    for shape in prs.slides[0].shapes:
        if shape.has_text_frame:
            out.append(shape.text_frame.text)
    return out


# ---------------------------------------------------------------------------
# 1. Currency-correctness
# ---------------------------------------------------------------------------
class TestCurrencyResolution:
    def test_defaults_to_usd_when_unknown(self):
        assert ppt._plan_currency_code({}) == "USD"
        assert ppt._plan_currency_code(None) == "USD"
        assert ppt._plan_currency_code({"locations": []}) == "USD"

    def test_resolves_country_from_locations(self):
        assert ppt._plan_currency_code({"locations": ["United Kingdom"]}) == "GBP"
        assert ppt._plan_currency_code({"locations": ["Germany"]}) == "EUR"
        assert ppt._plan_currency_code({"locations": ["Mumbai, India"]}) == "INR"

    def test_resolves_dict_locations(self):
        data = {"locations": [{"city": "London", "country": "United Kingdom"}]}
        assert ppt._plan_currency_code(data) == "GBP"

    def test_explicit_currency_code_wins(self):
        assert ppt._plan_currency_code({"currency_code": "eur"}) == "EUR"
        assert (
            ppt._plan_currency_code({"currency": "JPY", "locations": ["United States"]})
            == "JPY"
        )

    def test_set_active_currency_stashes_on_data(self):
        data = {"locations": ["United Kingdom"]}
        code = ppt._set_active_currency(data)
        assert code == "GBP"
        assert data["_plan_currency_code"] == "GBP"
        # restore default for other tests
        ppt._set_active_currency({})


class TestCurrencyFormatting:
    def teardown_method(self):
        ppt._set_active_currency({})  # reset to USD between tests

    def test_fmt_currency_uses_active_currency(self):
        ppt._set_active_currency({"locations": ["United Kingdom"]})
        assert ppt._fmt_currency(150000) == "£150,000"
        assert ppt._fmt_currency(1_200_000, compact=True) == "£1.2M"

    def test_fmt_currency_explicit_override_is_usd(self):
        ppt._set_active_currency({"locations": ["United Kingdom"]})
        # benchmark figures opt back into USD explicitly
        assert ppt._fmt_currency(35, currency="USD") == "$35"

    def test_format_budget_display_localizes(self):
        ppt._set_active_currency({"locations": ["Germany"]})
        assert ppt._format_budget_display("$150,000") == "€150K"

    def test_format_salary_localizes(self):
        ppt._set_active_currency({"locations": ["India"]})
        assert ppt._format_salary(85000) == "₹85K"

    def test_none_and_default_unchanged(self):
        ppt._set_active_currency({})
        assert ppt._fmt_currency(None) == "—"
        assert ppt._fmt_currency(150000) == "$150,000"


class TestNonUsdDeckRendering:
    def test_uk_plan_renders_pounds_not_dollars(self):
        pptx_bytes = ppt.generate_pptx(_plan("United Kingdom"))
        texts = _all_text(pptx_bytes)
        # The plan's own money figures use the pound symbol...
        pound_cells = [t for t in texts if t.startswith("£")]
        assert pound_cells, "expected GBP-formatted money in a UK plan"
        # ...and the localized total investment hero is present.
        assert any("£150" in t for t in texts)
        ppt._set_active_currency({})  # reset

    def test_us_plan_still_uses_dollars(self):
        pptx_bytes = ppt.generate_pptx(_plan("United States"))
        texts = _all_text(pptx_bytes)
        assert any(t.startswith("$") and "," in t for t in texts)


# ---------------------------------------------------------------------------
# 2. Brand chart fonts
# ---------------------------------------------------------------------------
class TestChartFonts:
    def test_bundled_poppins_files_exist(self):
        fonts_dir = PROJECT_ROOT / "fonts"
        for name in (
            "Poppins-Regular.ttf",
            "Poppins-SemiBold.ttf",
            "Poppins-Bold.ttf",
        ):
            assert (fonts_dir / name).is_file(), f"missing bundled font {name}"

    def test_register_returns_poppins_when_present(self):
        if not ppt._HAS_MATPLOTLIB:
            pytest.skip("matplotlib not installed")
        assert ppt._register_chart_fonts() == "Poppins"

    def test_register_falls_back_to_dejavu_when_dir_missing(self):
        # Point the resolver at a directory with no fonts -> graceful fallback.
        with mock.patch.object(ppt, "_FONTS_DIR", PROJECT_ROOT / "no_such_fonts_dir"):
            assert ppt._register_chart_fonts() == "DejaVu Sans"

    def test_pie_chart_still_renders(self):
        if not ppt._HAS_MATPLOTLIB:
            pytest.skip("matplotlib not installed")
        png = ppt._generate_pie_chart_image(["Indeed", "LinkedIn"], [60.0, 40.0])
        assert png and png[:4] == b"\x89PNG"


# ---------------------------------------------------------------------------
# 3. Data-sources slide freshness + provenance + measured callout
# ---------------------------------------------------------------------------
class TestDataSourcesSlide:
    def teardown_method(self):
        ppt._set_active_currency({})

    def test_freshness_line_present(self):
        today = datetime.date.today().strftime("%B %d, %Y")
        texts = _data_sources_text({"client_name": "Acme"})
        assert any(f"Data current as of {today}" in t for t in texts)

    def test_provenance_footer_present(self):
        texts = _data_sources_text({"client_name": "Acme"})
        assert any(t.lower().startswith("provenance:") for t in texts)

    def test_measured_callout_absent_by_default(self):
        # No real_outcomes -> no "Joveo measured" callout (common no-match case).
        texts = _data_sources_text({"client_name": "Acme"})
        assert not any("Joveo measured" in t for t in texts)

    def test_measured_callout_present_with_real_outcomes(self):
        data = {
            "client_name": "Acme",
            "_budget_allocation": {
                "metadata": {
                    "real_outcomes": [
                        {"title": "Registered Nurse", "sample_size": 300},
                        {"title": "ICU Nurse", "sample_size": 120},
                    ]
                }
            },
        }
        texts = _data_sources_text(data)
        assert any("Joveo measured" in t for t in texts)
        assert any("2 roles matched" in t for t in texts)
        # provenance footer also cites the warehouse calibration
        assert any("cg_benchmarks" in t for t in texts)

    def test_malformed_budget_allocation_is_safe(self):
        # Defensive reads: metadata not a dict, real_outcomes missing, etc.
        for bad in (None, [], "x", {"metadata": None}, {"metadata": "x"}):
            texts = _data_sources_text(
                {"client_name": "Acme", "_budget_allocation": bad}
            )
            assert not any("Joveo measured" in t for t in texts)


# ---------------------------------------------------------------------------
# 4. Channel table totals row
# ---------------------------------------------------------------------------
class TestTotalsRow:
    def teardown_method(self):
        ppt._set_active_currency({})

    def test_budget_slide_has_totals_row(self):
        data = _plan("United States")
        ppt._set_active_currency(data)
        prs = Presentation()
        prs.slide_width = ppt.SLIDE_WIDTH
        prs.slide_height = ppt.SLIDE_HEIGHT
        ppt._build_slide_budget_allocation(prs, data)
        texts = []
        for shape in prs.slides[0].shapes:
            if shape.has_text_frame:
                texts.append(shape.text_frame.text)
        assert any(t.strip() == "Total" for t in texts), "missing totals row"


# ---------------------------------------------------------------------------
# End-to-end: deck still builds from a minimal dict
# ---------------------------------------------------------------------------
class TestEndToEnd:
    def teardown_method(self):
        ppt._set_active_currency({})

    def test_minimal_dict_builds_valid_pptx(self):
        data = {
            "client_name": "Acme Co",
            "industry": "healthcare",
            "budget": "$100,000",
            "locations": ["New York, NY"],
            "roles": ["Registered Nurse"],
        }
        pptx_bytes = ppt.generate_pptx(data)
        assert isinstance(pptx_bytes, bytes) and len(pptx_bytes) > 10_000
        # Valid .pptx is a zip archive (PK magic bytes).
        assert pptx_bytes[:2] == b"PK"
        prs = Presentation(io.BytesIO(pptx_bytes))
        assert len(prs.slides) >= 5

    def test_full_sample_shape_builds(self):
        pptx_bytes = ppt.generate_pptx(_plan("United States"))
        assert pptx_bytes[:2] == b"PK"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
