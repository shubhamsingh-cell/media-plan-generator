"""G6: the Population column names the level of every figure and drops no site.

RTI audit (2026-10-11): the column mixed a census CITY (Philadelphia 1,603,797)
with METRO figures (Columbus 2,100,000), gave the state entry "NY" New York
CITY's 8,336,817, and ``api_enrichment`` silently looked up only the first 10
locations (GeoNames fallback table, live GeoNames, Teleport).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(PROJECT_ROOT), str(PROJECT_ROOT / "tests")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import api_enrichment  # noqa: E402
import data_synthesizer  # noqa: E402
import geo_audit_helpers as h  # noqa: E402


def _fuse(raw_locations, enriched):
    locations, sidecar = h.canonical_plan_locations(raw_locations)
    return data_synthesizer.fuse_location_profiles(
        enriched, {}, {"locations": locations, "_location_resolution": sidecar}
    )


def test_state_entry_never_inherits_a_citys_population():
    nyc = 8_336_817
    enriched = {
        "location_demographics": {
            "New York": {"population": nyc, "median_income": 76_607, "geo_level": "city"}
        },
        "geonames_data": {"locations": {"New York": {"population": nyc, "latitude": "40.7", "longitude": "-74.0"}}},
    }
    profile = _fuse(["NY"], enriched)["New York"]
    assert profile["population"] != nyc
    assert profile["population_level"] == "state"
    assert profile["population"] == 19_500_000  # STATE_DATA: New York statewide


def test_each_population_figure_is_tagged_with_its_level():
    enriched = {
        "location_demographics": {
            "Philadelphia, PA": {"population": 1_603_797, "median_income": 57_537, "geo_level": "city"}
        }
    }
    profiles = _fuse(["Philadelphia, PA", "Pittsburgh, PA", "PA"], enriched)
    assert profiles["Philadelphia, PA"]["population_level"] == "city"
    assert profiles["Pittsburgh, PA"]["population_level"] == "metro"  # METRO_DATA fallback
    assert profiles["Pennsylvania"]["population_level"] == "state"


def test_workbook_population_column_labels_every_figure():
    wb = h.workbook(h.build_plan(h.RTI_LOCATIONS))
    rows = h.intelligence_table(wb)
    for loc, row in rows.items():
        cell = row["Population"]
        assert cell == "—" or cell.endswith(("(city)", "(metro)", "(state)")), (loc, cell)
    assert rows["New York"]["Population"].endswith("(state)")
    assert "8,336,817" not in rows["New York"]["Population"]
    assert rows["Columbus"]["Population"].endswith("(metro)")


def test_local_lookups_cover_every_location_not_the_first_ten():
    cities = list(api_enrichment._HARDCODED_GEO)[:14]
    assert len(api_enrichment._geonames_hardcoded_lookup(cities)["locations"]) == 14
    teleport = [c for c in api_enrichment.TELEPORT_SLUGS if api_enrichment.TELEPORT_SLUGS[c] in api_enrichment._TELEPORT_BENCHMARK_DATA]
    assert len(teleport) >= 12


def test_teleport_curated_lookup_has_no_silent_cap(monkeypatch):
    monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, data: None)
    cities = [
        c
        for c in api_enrichment.TELEPORT_SLUGS
        if api_enrichment.TELEPORT_SLUGS[c] in api_enrichment._TELEPORT_BENCHMARK_DATA
    ][:12]
    result = api_enrichment.fetch_teleport_city_data(cities)
    assert len(result["cities"]) == 12


def test_live_geonames_cap_is_kept_for_api_cost_but_disclosed(monkeypatch):
    monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, data: None)
    monkeypatch.setenv("GEONAMES_USERNAME", "test-user")
    monkeypatch.setattr(api_enrichment, "_http_get_json", lambda *a, **k: {})
    cities = list(api_enrichment._HARDCODED_GEO)[:13]
    result = api_enrichment.fetch_geonames_data(cities)
    # the three locations past the live cap still come from the local table ...
    assert set(cities[10:]) <= set(result["locations"])
    # ... and the cap itself is stated, not silent
    cap = result["live_lookup_cap"]
    assert cap["limit"] == 10 and cap["requested"] == 13
    assert cap["local_data_only"] == cities[10:]
