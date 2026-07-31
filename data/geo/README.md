# US location data (`data/geo/`)

Committed, tab-separated reference tables for offline US location
resolution (states, counties, places/cities, ZIP codes). Built by
`scripts/build_geo_data.py` from US Census Bureau public files. This
directory is **data only** -- there is no runtime resolver code here (that
is a separate workstream); these files are inputs for one.

**License**: all source files are published by the US Census Bureau, a US
federal government agency. Per 17 U.S.C. 105, US federal government works
are not subject to copyright and are in the public domain. Free to use,
modify, and redistribute.

## Build command

```bash
python3 scripts/build_geo_data.py
# or, to force fresh downloads instead of the cached copies:
python3 scripts/build_geo_data.py --no-cache
```

Downloads are cached under `data/.geo_build_cache/` (gitignored) so re-runs
are fast and idempotent. The script fails loudly (raises `BuildError`) on
any HTTP error or unexpected column header -- it never emits a silently
truncated file.

## Vintage

- Gazetteer files: **2024 Gazetteer** (`2024_Gazetteer` vintage).
- Relationship / reference files: **2020 Census** vintage (`rel2020`,
  `codes2020`). This is the newest complete relationship/reference series
  Census has published as of this build (2026-07-31); the Gazetteer series
  is refreshed annually and pairs with whichever relationship vintage is
  current.
- Built: 2026-07-31.

## Sources

| File | URL | Used for |
|---|---|---|
| `2024_Gaz_place_national.zip` | `https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_place_national.zip` | place names, lat/lng |
| `2024_Gaz_counties_national.zip` | `https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_counties_national.zip` | county names, FIPS, lat/lng |
| `2024_Gaz_zcta_national.zip` | `https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_zcta_national.zip` | ZIP (ZCTA5) lat/lng |
| `national_place2020.txt` | `https://www2.census.gov/geo/docs/reference/codes2020/national_place2020.txt` | place -> county name(s) |
| `national_state2020.txt` | `https://www2.census.gov/geo/docs/reference/codes2020/national_state2020.txt` | state USPS <-> full name |
| `tab20_zcta520_county20_natl.txt` | `https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt` | ZIP -> county |
| `tab20_zcta520_place20_natl.txt` | `https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_place20_natl.txt` | ZIP -> primary city |

The zip->county relationship file was found by listing
`https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/` with
curl on 2026-07-31 and verifying HTTP 200 -- the URL was not guessed. The
same directory also has the ZCTA->place file used for `primary_city`,
verified the same way. All seven URLs above returned HTTP 200 at build
time.

## Files produced

- `us_states.tsv` -- `state_usps, state_name` (57 rows: 50 states + DC +
  5 territories AS/GU/MP/PR/VI/UM per the Census state reference file).
- `us_counties.tsv` -- `county_fips (5-digit zero-padded), county_name,
  state_usps, lat, lng` (3,222 rows -- includes county-equivalents:
  parishes (LA), boroughs/census areas (AK), independent cities (VA), etc.
  under their Census `NAME`, unmodified).
- `us_places.tsv` -- `place_key, display_name, state_usps, county_fips, lat,
  lng, place_type, county_confidence` (31,657 rows). `place_key` is
  `<lowercased/punct-stripped/whitespace-collapsed display_name>|<lowercase
  state_usps>` -- **scoped per state**, so cross-state name collisions
  (many "Springfield"s) never collide with each other, but two identically
  named places in the *same* state still collide (rare; not observed in
  this vintage). `place_type` is the trailing legal/statistical-area
  qualifier stripped off of `display_name` (see "Judgment calls" below);
  it is `"CDP"` when no distinguishing suffix was present. `county_confidence`
  is `"zip_majority"` or `"census_first_listed"` -- see judgment call #2,
  which was corrected 2026-07-31 after a shipped-data review caught it
  picking the wrong county for essentially every major multi-county city.
- `us_zips.tsv` -- `zip5 (5-digit zero-padded), primary_city, state_usps,
  county_fips, lat, lng` (33,486 rows).

Row counts will drift slightly on every re-run as Census updates the
underlying files; the integrity test pins generous ranges, not exact
counts.

## Judgment calls

1. **`display_name` suffix stripping.** Census place names carry a
   trailing legal/statistical-area type (`"Atlanta city"`, `"Cary town"`,
   `"Calhoun CDP"`). We strip a fixed list of generic suffixes --
   `cdp, city, town, village, borough, township, municipality,
   corporation` -- plus a trailing `" (balance)"` qualifier, to get a
   clean display name. We deliberately do **not** strip `county`,
   `parish`, `unified government`, `consolidated government`,
   `metropolitan government`/`metro government`, or `urban county` --
   those words are load-bearing parts of a real, informative name for
   consolidated city-county governments, e.g. `"Athens-Clarke County
   unified government"`, `"Louisville/Jefferson County metro
   government"`, `"Macon-Bibb County"`. Stripping them would produce a
   misleading or ambiguous display name, so those rows keep their full
   Census name verbatim (`place_type` for those rows is whatever trailing
   token pattern-matched, e.g. `"(balance)"`, or the row falls through
   with no suffix stripped at all).

   The suffix match is **case-sensitive, deliberately**. Census writes the
   legal type in lowercase in `NAME` (`"Atlanta city"`, `"Boise City
   city"`) but capitalises the word when it is part of the proper name.
   The first build matched case-insensitively and reduced Nevada's capital
   `"Carson City"` to `"Carson"`. `CDP` is the one type Census writes in
   uppercase, so it is matched explicitly rather than folded into the
   lowercase list. Pinned by
   `tests/test_plan_location.py::test_carson_city_display_name_keeps_its_own_city_word`.
2. **Place -> county join, and multi-county places (corrected 2026-07-31).**
   `national_place2020.txt`'s `COUNTIES` column lists a place's
   constituent counties `~~~`-joined for multi-county places (e.g.
   `Birmingham city, AL` straddles Jefferson and Shelby counties). The
   first build of this pipeline took the **first-listed** county as
   primary -- but that list turns out to be ordered essentially
   alphabetically by county FIPS, not by population or area share, so
   "first listed" silently picked the wrong county for most major
   multi-county cities: `Atlanta, GA` resolved to DeKalb (13089) instead of
   Fulton (13121); `Houston, TX` to Fort Bend (48157) instead of Harris
   (48201); `Dallas, TX` to Collin (48085) instead of Dallas (48113);
   `Kansas City, MO` to Cass (29037) instead of Jackson (29095);
   `Oklahoma City, OK` to Canadian (40017) instead of Oklahoma (40109);
   `Columbus, OH` to Delaware (39041) instead of Franklin (39049). Since
   this dataset feeds a client-facing "we understood your location as X"
   echo-back, an unqualified wrong-county answer is a trust-destroying
   defect, not an acceptable caveat.

   **Fix**: `scripts/build_geo_data.py`'s
   `refine_place_counties_by_zip_majority()` runs a post-join refinement
   pass. `us_zips.tsv`'s per-ZIP `county_fips` (the largest-land-area-overlap
   join described in judgment call #4) does not have this defect, so for
   every place we take a **majority vote over the ZCTAs whose
   `primary_city`+`state_usps` match that place** (normalized: lowercased,
   whitespace-collapsed) and use the county most ZIPs agree on, overwriting
   the Census first-listed county. Verified against the 10 largest
   multi-county US cities plus Chicago/Los Angeles/Phoenix/Seattle: 10/10
   correct after the fix (see
   `tests/test_geo_data_integrity.py::test_multi_county_city_county_fips_regression_pins`).
   Ties are broken deterministically by preferring the smaller
   `county_fips`.

   **Fallback**: a place with no matching ZIP (e.g. a tiny CDP that never
   won the largest-overlap tie-break for any ZIP) keeps its original
   Census first-listed county rather than being dropped. `county_confidence`
   records which path a row took: `"zip_majority"` (the corrected, high-
   confidence assignment) or `"census_first_listed"` (the original,
   lower-confidence assignment -- still usually correct for single-county
   places, since the "first-listed" defect only bites when there IS more
   than one county to choose between). On the 2026-07-31 build: 22,550
   places (71.2%) resolved via `zip_majority`, 9,107 (28.8%) fell back to
   `census_first_listed` -- consistent with the fallback rate being
   dominated by small single-county CDPs and towns that never fell into a
   dispute in the first place, not by unresolved multi-county ambiguity.
   Downstream consumers that need the higher-confidence subset should
   filter on `county_confidence == "zip_majority"`.

   We also considered (per the fix brief) matching a ZIP's `primary_city`
   against a *separately* de-suffixed place name, to rescue places like
   `Nashville-Davidson metropolitan government` whose Census name doesn't
   look like a plain city name. This turned out to be unnecessary:
   `us_zips.tsv`'s `primary_city` is derived by running the exact same
   `strip_legal_suffix()` function used to produce a place's
   `display_name`, so the two strings are already identical whenever they
   refer to the same place -- Nashville, TN resolves via `zip_majority`
   with no special-casing required. No second matching pass was added, to
   avoid introducing a looser match that could produce false positives.
3. **Vintage-mismatch drops (loud, not silent).** `national_place2020.txt`
   is a 2020-vintage reference file; the 2024 Gazetteer place file
   includes newly incorporated/renamed places (mostly small CDPs) that
   don't exist in the 2020 file yet. Places (or ZIPs, transitively) that
   can't resolve a county through this join are **excluded** from the
   output -- but the count is printed in the build summary
   (`places_skipped`, `zips_skipped`), never silently dropped. On the
   2026-07-31 build this was 676 of 32,333 places (~2%) and 305 of 33,791
   ZCTAs (~0.9%).
4. **ZIP -> county / ZIP -> primary_city, and split ZIPs.** Some ZCTAs
   (Census's ZIP-code proxy) span more than one county or more than one
   place. For each ZIP we pick the county and the place with the
   **largest land-area overlap** (`AREALAND_PART` in the relationship
   files), i.e. the plurality-area component. A ZIP genuinely split
   across two cities/counties is represented by one row naming its
   largest-share city/county, not by two rows.
5. **ZCTA != USPS ZIP.** Census's ZCTA (ZIP Code Tabulation Area) is a
   geographic proxy for USPS ZIP codes, not an exact 1:1 mapping. A small
   number of real USPS ZIP codes -- mostly PO-Box-only or unique-purpose
   ZIPs with no residential/business footprint -- have **no** corresponding
   ZCTA and therefore do not appear in `us_zips.tsv`. Example: Atlanta,
   GA's downtown PO-Box ZIP `30301` is not a ZCTA in the 2024 Gazetteer
   (confirmed absent from `2024_Gaz_zcta_national.txt`); the nearby
   ZCTA-backed Atlanta ZIP `30303` is used for spot-checks instead. This
   is an inherent limitation of any ZCTA-based ZIP dataset, not a bug in
   this build.

## Zero-padding

`county_fips` and `zip5` are always emitted, and MUST always be read back,
as zero-padded strings (`"01001"`, `"00601"`), never as integers -- leading
zeros are significant (Connecticut counties, Puerto Rico ZIPs, etc.). Both
the build script and `tests/test_geo_data_integrity.py` assert every value
is exactly 5 characters and all-digits.
