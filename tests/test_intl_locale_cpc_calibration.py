"""Regression tests for Fix 1: non-US locale CPC calibration.

Bug fixed: budget_engine.py priced EVERY plan's CPC/CPA off US-calibrated
cost curves (BASE_BENCHMARKS, channel_benchmarks_live.json, trend_engine,
the KB) regardless of the plan's actual markets -- a GBP plan for six
non-US markets (UK/Australia/Mexico/Argentina/Canada/New Zealand) was priced
off US Indeed/LinkedIn/ZipRecruiter benchmarks, i.e. "US economics wearing a
local currency symbol", with no FX and no local calibration.

Fix: ``intl_benchmark_lookup.get_locale_cpc_basis`` derives a per-category
CPC basis from ``data/international_benchmarks_2026.json`` (38 countries,
real per-platform CPC/CPA/market-share data), weighted by each platform's
own ``market_share_pct`` within a country and blended equally across the
plan's matched countries. ``budget_engine._resolve_intl_cpc_basis`` gates
this on the canonical ``plan_geo.is_us_plan`` (never a new detector) and
slots it into ``compute_channel_dollar_amounts``'s CPC cascade as a NEW
tier ABOVE synthesized/live_benchmark/trend_engine/KB/static -- but ONLY
for non-US plans.

Covers:
    1. ``get_locale_cpc_basis`` unit behavior: single-country local-currency
       basis, multi-country USD-blend basis, no-match returns None, no
       exchange rate fabricated.
    2. ``calculate_budget_allocation`` end-to-end wiring: a non-US plan's
       job_board/social channels resolve their CPC from the international
       basis (cpc_source starts with "intl_"), with real, non-US-blind
       values.
    3. THE MOST IMPORTANT TEST: a US-plan false-positive guard -- a US plan
       (with or without the new ``locations_raw``/``plan_currency`` params)
       produces a BYTE-IDENTICAL ``channel_allocations`` result to a call
       that doesn't know these new params exist at all. Fix 1 must never
       move a single US plan's numbers.

Runs under pytest, or standalone: ``python3 tests/test_intl_locale_cpc_calibration.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pytest  # noqa: E402

import budget_engine as be  # noqa: E402
import intl_benchmark_lookup as ibl  # noqa: E402


# ---------------------------------------------------------------------------
# 1. intl_benchmark_lookup.get_locale_cpc_basis -- unit behavior
# ---------------------------------------------------------------------------
class TestGetLocaleCpcBasis:
    def test_single_country_matching_currency_uses_local_figures(self):
        """A GBP plan whose only non-US market is the UK gets the UK
        platforms' own cpc_local median -- genuinely GBP-calibrated, no
        conversion needed or applied."""
        result = ibl.get_locale_cpc_basis(["UK"], "GBP")
        assert result is not None
        assert result["basis"] == "local"
        assert result["matched_countries"] == ["uk"]
        assert result["source"] == "intl_local:uk"
        # UK job_board platforms: Indeed UK/Reed/Totaljobs/CV-Library all
        # cpc_local (GBP) -- share-weighted median should land well under
        # £1, not a US-dollar figure.
        assert 0 < result["categories"]["job_board"] < 2.0

    def test_single_country_mismatched_currency_uses_usd_blend(self):
        """Same single UK market, but plan_currency is USD (not GBP) --
        cpc_local would be the WRONG unit for a USD-denominated budget, so
        the basis falls back to the dataset's own pre-computed cpc_usd."""
        result = ibl.get_locale_cpc_basis(["UK"], "USD")
        assert result is not None
        assert result["basis"] == "usd_blend"
        assert result["source"] == "intl_usd_blend:uk"

    def test_multi_country_mixed_currency_blends_usd_no_fx_invented(self):
        """The brief's own scenario: GBP/AUD/MXN/ARS/CAD/NZD, no single
        currency matches all six -- must use the usd_blend basis (the
        dataset's own cpc_usd fields), never a fabricated exchange rate."""
        countries = ["UK", "Australia", "Mexico", "argentina", "canada", "new zealand"]
        result = ibl.get_locale_cpc_basis(countries, "GBP")
        assert result is not None
        assert result["basis"] == "usd_blend"
        assert set(result["matched_countries"]) == {
            "uk",
            "australia",
            "mexico",
            "argentina",
            "canada",
            "new_zealand",
        }
        assert result["source"] == (
            "intl_usd_blend:uk,australia,mexico,argentina,canada,new_zealand"
        )
        # job_board and social (professional_network) are the only
        # categories these 6 markets' platform types map to in the dataset
        # -- never fabricate niche_board/regional/programmatic figures with
        # no source.
        assert set(result["categories"].keys()) == {"job_board", "social"}
        assert result["categories"]["job_board"] > 0
        assert result["categories"]["social"] > 0

    def test_no_country_match_returns_none(self):
        assert ibl.get_locale_cpc_basis(["Narnia", "Atlantis"], "GBP") is None

    def test_empty_or_none_countries_returns_none(self):
        assert ibl.get_locale_cpc_basis([], "GBP") is None
        assert ibl.get_locale_cpc_basis(None, "GBP") is None

    def test_us_not_in_dataset(self):
        # Defense in depth: the 38-country dataset has no "us"/"usa" slug at
        # all (it's the non-US dataset), so even a caller that forgets the
        # is_us_plan gate gets None back rather than a fabricated match.
        assert ibl.get_locale_cpc_basis(["United States", "USA"], "USD") is None


# ---------------------------------------------------------------------------
# Shared fixtures for calculate_budget_allocation integration tests
# ---------------------------------------------------------------------------
_ROLES = [{"title": "Commercial Cab Driver", "count": 500, "tier": "Hourly"}]
_CHANNEL_PCTS = {
    "programmatic_dsp": 30,
    "global_boards": 25,
    "niche_boards": 15,
    "social_media": 12,
    "regional_boards": 13,
    "employer_branding": 5,
}
_UBER_LOCATIONS_RAW = ["UK", "Australia", "Mexico", "argentina", "canada", "new zealand"]


def _non_us_alloc(**overrides):
    locs_for_ba = [
        {"city": loc, "state": "", "country": "US"} for loc in _UBER_LOCATIONS_RAW
    ]
    kwargs = dict(
        total_budget=2_000_000,
        roles=_ROLES,
        locations=locs_for_ba,
        industry="hospitality_travel",
        channel_percentages=dict(_CHANNEL_PCTS),
        collar_type="blue_collar",
        locations_raw=_UBER_LOCATIONS_RAW,
        plan_currency="GBP",
    )
    kwargs.update(overrides)
    return be.calculate_budget_allocation(**kwargs)


# ---------------------------------------------------------------------------
# 2. calculate_budget_allocation wiring -- non-US plan resolves the intl basis
# ---------------------------------------------------------------------------
class TestNonUsPlanWiring:
    def test_locations_raw_reveals_non_us_where_reshaped_locations_would_not(self):
        """The reshaped `locations` param (city/state/country dicts with
        country defaulted to "US" for bare, comma-less tokens like "UK")
        would make plan_geo.is_us_plan say True -- exactly the bug
        `locations_raw` exists to route around. Confirm the metadata shows
        a real basis was resolved, proving the plan was correctly read as
        non-US despite that reshape."""
        result = _non_us_alloc()
        basis = result["metadata"]["intl_cpc_basis"]
        assert basis is not None, "expected a resolved intl CPC basis for a non-US plan"
        assert basis["basis"] == "usd_blend"
        assert set(basis["matched_countries"]) == {
            "uk",
            "australia",
            "mexico",
            "argentina",
            "canada",
            "new_zealand",
        }

    def test_job_board_and_social_channels_use_intl_cpc_source(self):
        result = _non_us_alloc()
        ca = result["channel_allocations"]
        assert ca["global_boards"]["cpc_source"].startswith("intl_usd_blend:")
        assert ca["social_media"]["cpc_source"].startswith("intl_usd_blend:")

    def test_categories_with_no_intl_coverage_fall_through_to_existing_cascade(self):
        """programmatic and employer_branding have no matching platform type
        in these 6 markets' data -- they must fall through to the
        PRE-EXISTING cascade (never a fabricated non-US figure), same as
        before this fix."""
        result = _non_us_alloc()
        ca = result["channel_allocations"]
        assert not ca["programmatic_dsp"]["cpc_source"].startswith("intl_")
        assert not ca["employer_branding"]["cpc_source"].startswith("intl_")

    def test_locale_calibrated_job_board_cpc_is_lower_than_us_live_benchmark(self):
        """The real-world effect this fix exists to produce: real UK/AU/MX/
        AR/CA/NZ job-board CPCs (several of which are much cheaper markets
        than the US) blend to a lower, more realistic figure than the
        US-centric live_benchmark tier (channel_benchmarks_live.json:
        Indeed/ZipRecruiter/Glassdoor/Monster/CareerBuilder)."""
        result = _non_us_alloc()
        intl_cpc = result["channel_allocations"]["global_boards"]["cpc"]
        us_cpc = be._extract_cpc_from_live_benchmarks("job_board")
        if us_cpc is not None:
            assert intl_cpc < us_cpc

    def test_no_intl_basis_when_locations_raw_omitted_and_reshape_hides_country(self):
        """Without locations_raw, this function falls back to the (lossy)
        `locations` param -- which, for this fixture's bare-token reshape,
        defaults country to "US" and so reads as a US plan. No basis
        resolved, and no channel gets an intl_ cpc_source -- confirms the
        fallback path is inert, not a silent crash."""
        result = _non_us_alloc(locations_raw=None, plan_currency=None)
        assert result["metadata"]["intl_cpc_basis"] is None
        for ch in result["channel_allocations"].values():
            assert not str(ch.get("cpc_source") or "").startswith("intl_")


# ---------------------------------------------------------------------------
# 3. US-plan false-positive guard -- THIS MATTERS MOST
# ---------------------------------------------------------------------------
class TestUsPlanUnaffected:
    """Fix 1 must be a no-op, byte-identical, for every US plan -- whether
    or not a caller passes the new locations_raw/plan_currency params."""

    _US_ROLES = [{"title": "CDL A Driver", "count": 300, "tier": "Hourly"}]
    _US_LOCATIONS_RAW = [
        "Massachusetts",
        "Maine",
        "New Hampshire",
        "Rhode Island",
        "Connecticut",
        "Denver, CO",
    ]
    _US_CHANNEL_PCTS = {
        "programmatic_dsp": 25,
        "global_boards": 20,
        "niche_boards": 20,
        "social_media": 10,
        "regional_boards": 20,
        "employer_branding": 5,
    }

    def _us_locations_for_ba(self):
        out = []
        for loc in self._US_LOCATIONS_RAW:
            parts = [p.strip() for p in loc.split(",")]
            out.append(
                {
                    "city": parts[0],
                    "state": parts[1] if len(parts) > 1 else "",
                    "country": "US",
                }
            )
        return out

    def _base_kwargs(self):
        return dict(
            total_budget=150_000,
            roles=self._US_ROLES,
            locations=self._us_locations_for_ba(),
            industry="logistics_supply_chain",
            channel_percentages=dict(self._US_CHANNEL_PCTS),
            collar_type="blue_collar",
        )

    def test_new_params_omitted_entirely_still_works(self):
        """A caller that has never heard of Fix 1 (no locations_raw, no
        plan_currency kwargs at all) gets exactly the same result as
        before -- the new params are additive-only."""
        result = be.calculate_budget_allocation(**self._base_kwargs())
        assert result["metadata"]["intl_cpc_basis"] is None

    def test_locations_raw_and_plan_currency_supplied_no_effect(self):
        """Even when a caller DOES pass the real, unreshaped location
        strings and a plan currency for a genuinely US plan, is_us_plan
        correctly reads it as US and the result is untouched."""
        no_new_params = be.calculate_budget_allocation(**self._base_kwargs())
        with_new_params = be.calculate_budget_allocation(
            **self._base_kwargs(),
            locations_raw=self._US_LOCATIONS_RAW,
            plan_currency="USD",
        )
        assert with_new_params["metadata"]["intl_cpc_basis"] is None
        # Byte-identical channel_allocations -- the exact numbers, not just
        # "close enough".
        assert no_new_params["channel_allocations"] == with_new_params["channel_allocations"]
        assert no_new_params["total_projected"] == with_new_params["total_projected"]

    def test_single_us_state_bare_token_not_misread_as_non_us(self):
        """A bare, comma-less US state name/token (e.g. "Texas") must never
        accidentally match the international dataset or flip is_us_plan --
        the canonical plan_geo detector already handles this; confirm Fix 1
        doesn't introduce a second, disagreeing path."""
        result = be.calculate_budget_allocation(
            total_budget=100_000,
            roles=self._US_ROLES,
            locations=[{"city": "Texas", "state": "", "country": "US"}],
            industry="logistics_supply_chain",
            channel_percentages={"job_boards": 60, "social_media": 40},
            locations_raw=["Texas"],
            plan_currency="USD",
        )
        assert result["metadata"]["intl_cpc_basis"] is None


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
