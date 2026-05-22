# OECD SDMX API -- Discovery Spike

**Date:** 2026-05-22
**Author:** Discovery spike (Nova AI Suite, F4 audit follow-up)
**Status:** Spike COMPLETE -- recommend proceeding to build (4 days, not 4-6)
**Decision:** GO -- API is healthy and richly useful for Nova's recruitment context

---

## TL;DR

The F4 audit's 404s were caused by **wrong URL pattern + wrong dimension count**, not a broken API. The OECD SDMX API at `https://sdmx.oecd.org/public/rest/` is fully operational, returns SDMX-JSON v2 or labelled CSV, and exposes 5 datasets directly relevant to Joveo (unemployment, employment+working-age population, wages, hours worked, migration, productivity).

**Live evidence (run 2026-05-22):** G5 unemployment Q1 2026 returned in 2.7 seconds (10 KB): US 4.3%, UK 4.9%, Germany 4.0%, France 7.8%, Japan 2.7%. USA average annual wages 2020-2023 in USD PPP: $82,783 -> $84,211 -> $81,838 -> $82,078.

A standalone working module exists at `docs/oecd_sdmx_sample.py`. It has a single public function `query_oecd(country, dataset, start_year, end_year)` ready to be wired into `nova.py` alongside `_query_eurostat` and `_query_uk_ons`. Total implementation effort: ~1.5 days code + 0.5 day test + 0.5 day doc = **2.5 days**, with another 1.5 days of dataset catalogue expansion if we want to scale beyond the initial 5.

**Recommendation:** Proceed. Spike used 60 min of the 2-day budget; refund the remainder.

---

## Phase 1 -- Which API is current?

### The migration

The OECD ran two parallel statistical APIs through 2024 and consolidated to one in mid-2024. The result of the audit's two probed paths:

| Host | Status (May 2026) | Notes |
|---|---|---|
| `stats.oecd.org/sdmx-json/data/` | **DEPRECATED** -- HEAD returns 405; GET hangs after partial response | Legacy OECD.Stat. Audit's 404s likely came from probing here. |
| `sdmx.oecd.org/public/rest/data/` | **CURRENT** -- HTTP 200, 2-3s typical latency | New OECD Data Explorer API. SDMX 2.1 / SDMX-JSON 2.0. |

The legacy stats.oecd.org host is being kept barely alive for inertia traffic but should not be used for new integrations. The official migration guide is at [gitlab.algobank.oecd.org/.../OECD_Data_API_documentation-Upgrading_from_the_legacy_OECD.Stat_APIs.pdf](https://gitlab.algobank.oecd.org/public-documentation/dotstat-migration/-/raw/main/OECD_Data_API_documentation-Upgrading_from_the_legacy_OECD.Stat_APIs.pdf).

### Authentication & limits

- **Auth:** None. Public, anonymous GET only.
- **Rate limits:** Not published by OECD. Empirically, single-country queries finish in 2-3 s; broad pulls (5+ countries, no time bound) can take >30 s and time out at the network layer.
- **Hard limits:**
  - Max **1,000,000 observations per response**
  - Max **1,000 characters per request URL**
  - Response truncates without warning at the obs limit
- **SLA:** None. OECD provides the API free under their Terms (no uptime guarantee).
- **Documentation:** [oecd.org/en/data/insights/data-explainers/2024/09/api.html](https://www.oecd.org/en/data/insights/data-explainers/2024/09/api.html) (403 to WebFetch but accessible via browser)

### URL grammar

```
https://sdmx.oecd.org/public/rest/data/{AGENCY},{DATAFLOW},{VERSION}/{KEY}?startPeriod=&endPeriod=&dimensionAtObservation=AllDimensions
```

- `AGENCY` -- e.g. `OECD.SDD.TPS` (population/labour) or `OECD.ELS.SAE` (employment/social/earnings)
- `DATAFLOW` -- e.g. `DSD_LFS@DF_IALFS_UNE_M`. The `DSD_x@DF_y` form means "Data Structure Definition x referenced via Dataflow y".
- `VERSION` -- e.g. `1.0`. Optional, defaults to latest.
- `KEY` -- dot-separated dimension values. **The dot count is fixed per dataflow.** Empty between dots = "all values for that dimension". Multi-value via `+`: `USA+GBR+DEU`.

### Content negotiation

The `?format=` query parameter that the older docs reference works for browser previews but is *unreliable* for programmatic clients. Use the `Accept` header instead:

| Format | Accept header |
|---|---|
| **SDMX-JSON v2 (recommended for code)** | `application/vnd.sdmx.data+json; version=2` |
| CSV with labels | `application/vnd.sdmx.data+csv; labels=both; charset=utf-8` |
| CSV with IDs only | `application/vnd.sdmx.data+csv; labels=id; charset=utf-8` |
| Generic SDMX XML | `application/vnd.sdmx.genericdata+xml; version=2.1` |

Sending no Accept header (or `application/json`) yields HTTP 406 with the server returning the list of acceptable types as the response body -- useful for debugging but easy to trip.

---

## Phase 2 -- Recruitment-relevant datasets

Five datasets were probed live. All return data for major OECD countries.

| Slug (sample module key) | Agency | Dataflow | Version | Dim count | Freshness | Granularity |
|---|---|---|---|---|---|---|
| `unemployment_rate_monthly` | `OECD.SDD.TPS` | `DSD_LFS@DF_IALFS_UNE_M` | 1.0 | 9 | 2026-04 (T-1 month) | Monthly |
| `labour_force_indicators` | `OECD.SDD.TPS` | `DSD_LFS@DF_IALFS_INDIC` | 1.0 | 9 | 2025-Q2 (T-3 months) | Quarterly + Annual |
| `average_annual_wages` | `OECD.ELS.SAE` | `DSD_EARNINGS@AV_AN_WAGE` | 1.0 | 7 | 2024 (T-1 year) | Annual |
| `hours_worked` | `OECD.ELS.SAE` | `DSD_HW@DF_AVG_USL_WK_WKD` | 1.0 | 13 | 2024 | Annual |
| `productivity_levels` | `OECD.SDD.TPS` | `DSD_PDB@DF_PDB_LV` | 1.0 | 9 | 2024 | Annual |
| `migration_inflows` | `OECD.ELS.IMD` | `DSD_MIG@DF_MIG` | 1.0 | 8 | 2024 | Annual |

### Geography coverage

All 38 OECD member countries plus a sub-set of accession/key partners (Brazil, India, China, Indonesia, South Africa for some flows). Country codes are **ISO-3 alpha** (USA, GBR, DEU, FRA, JPN, IND, etc.). Joveo's primary markets -- US, UK, Germany, France, Italy, Spain, Netherlands, Belgium, Sweden, Australia, Canada, Japan -- are all covered.

### Why these six matter for Joveo

| Dataset | Recruitment use case |
|---|---|
| `unemployment_rate_monthly` | Macro context for fill-rate predictions, CPA calibration. Tighter labour markets -> higher CPC, longer time-to-fill. |
| `labour_force_indicators` | Working-age population trends -- input for TAM sizing in CG Automation and SlotOps planning |
| `average_annual_wages` | Salary anchoring when Nova doesn't have local salary data; sanity check on Mercer/Payscale fits |
| `hours_worked` | Full-time vs part-time benchmark; useful for gig/hourly segment plans |
| `productivity_levels` | Cost-per-hire benchmarking by country GDP-per-hour |
| `migration_inflows` | Talent supply forecasting -- which countries are net importers of working-age people |

### NOT available via OECD SDMX

The audit hoped for **job vacancy rates**. OECD does not publish a dedicated vacancy dataflow. STES (Short-Term Economic Statistics) under `OECD.SDD.STES` only exposes: `DF_KEI`, `DF_BTS`, `DF_CLI`, `DF_CS`, `DF_CSBAR`, `DF_FINMARK`, `DF_INDSERV`, `DF_MONAGG`, `DF_STES_REVISIONS` -- none are vacancies. For vacancy rates, Joveo should continue using Eurostat (EU JVS) and country-specific sources (BLS JOLTS for US, ONS for UK, Stats Canada).

### Probing for more datasets

To discover the agency/dataflow ID for any dataset in the Data Explorer UI:

1. Open [data-explorer.oecd.org](https://data-explorer.oecd.org/)
2. Find the dataset you want
3. Click the **Developer API** icon above the data table
4. Copy the URL it shows -- agency/dataflow/version are visible in the path

Or list everything via:
```bash
curl 'https://sdmx.oecd.org/public/rest/dataflow/all/all/latest' \
  -H 'Accept: application/vnd.sdmx.structure+xml; version=2.1' \
  -o /tmp/all_dataflows.xml
# ~8.5 MB, ~3,500 dataflows
```

---

## Phase 3 -- Live query proofs

All queries below were run from this machine on 2026-05-22. Responses snipped for brevity; full payloads available by re-running the curls.

### 3.1 -- Monthly unemployment rate (`DSD_LFS@DF_IALFS_UNE_M`)

```bash
curl -sSL -H "Accept: application/vnd.sdmx.data+csv; labels=both" \
  "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_UNE_M,1.0/USA+GBR+DEU+FRA+JPN._Z.Y._T.Y_GE15..M?startPeriod=2025-01"
```

HTTP 200, 23 KB. First rows:
```
DEU: Germany, UNE_LF_M: Monthly unemployment rate, PT_LF_SUB, _Z, Y: Seasonally adjusted, _T: Total, Y_GE15: 15+, M: Monthly, 2025-01, 3.5
DEU: Germany, ..., 2025-02, 3.6
DEU: Germany, ..., 2025-03, 3.6
...
```

JSON variant (same URL, `Accept: application/vnd.sdmx.data+json; version=2`):
```json
{
  "meta": {"id": "IREF012540", "prepared": "2026-05-22T11:14:48Z"},
  "data": {
    "dataSets": [{
      "series": {"0:0:0:0:0:0:0:0:0": {
        "observations": {"0": [4, 0], "1": [4.2, 0], "2": [4.2, 0]}
      }}
    }],
    "structures": [{"name": "Monthly unemployment rates", ...}]
  }
}
```

### 3.2 -- Average annual wages (`DSD_EARNINGS@AV_AN_WAGE`)

```bash
curl -sSL -H "Accept: application/vnd.sdmx.data+csv; labels=both" \
  "https://sdmx.oecd.org/public/rest/data/OECD.ELS.SAE,DSD_EARNINGS@AV_AN_WAGE,1.0/USA+GBR+DEU......?startPeriod=2020&endPeriod=2023"
```

HTTP 200, 7.6 KB. Sample:
```
DEU: Germany, WG: Wages, USD_PPP, A: Annual, Q: Constant prices, MEAN, _Z, 2020, 69750.812
DEU, ..., 2021, 69828.515
DEU, ..., 2022, 68225.562
DEU, ..., 2023, 68104.229
GBR: United Kingdom, ..., 2020, 62432.983
USA: United States, ..., 2020, 82783.413
USA, ..., 2023, 82078.428
```

### 3.3 -- Labour force indicators (`DSD_LFS@DF_IALFS_INDIC`)

```bash
curl -sSL -H "Accept: application/vnd.sdmx.data+csv; labels=both" \
  "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_INDIC,1.0/USA+GBR+DEU.........?startPeriod=2024"
```

HTTP 200, 7 MB (note the size -- ALWAYS bound time period for this dataflow). Sample:
```
DEU, WAP: Working-age population, PS: Persons, ..., Y55T64: 55-64, Q: Quarterly, 2025-Q2, 12999.83 (thousands)
GBR, EMP: Employment, ..., A: Agriculture/forestry/fishing, 2025-Q2, 272 (thousands)
USA, EMP: Employment, ..., A: Agriculture, 2024, 2254.083 (thousands)
```

### 3.4 -- Productivity levels (`DSD_PDB@DF_PDB_LV`)

```bash
curl -sSL -H "Accept: application/vnd.sdmx.data+csv; labels=both" \
  "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_PDB@DF_PDB_LV,1.0/USA+GBR+DEU+FRA+JPN........?lastNObservations=1"
```

HTTP 200, 28 KB. Sample:
```
DEU, HRSTO: Hours worked, _T (all activities), H: Hours, ..., 2024, 61364 (millions)
GBR, GDPPOP: GDP per capita, ..., XDC_PS: National currency per person, V: Current, 2024, 41183.79
FRA, GDPEMP: GDP per person employed, ..., USD_PPP_PS, 2024, 137131.68
USA, GDPHRS: GDP per hour worked, ..., 2023, 97.05
```

### 3.5 -- Migration inflows (`DSD_MIG@DF_MIG`)

```bash
curl -sSL -H "Accept: application/vnd.sdmx.data+csv; labels=both" \
  "https://sdmx.oecd.org/public/rest/data/OECD.ELS.IMD,DSD_MIG@DF_MIG,1.0/?lastNObservations=1"
```

HTTP 200, 14 MB (always filter by REF_AREA or by CITIZENSHIP). Sample rows:
```
FIN, citizenship=RUS, A: Annual, B11: Inflows of foreign population, F: Female, ..., 2023, 2096 persons
NLD, citizenship=ISL, ..., B11: Inflows, _T: Total, ..., 2023, 125 persons
HUN, citizenship=NZL, ..., B16: Acquisitions of nationality, ..., 2023, 2 persons
```

### 3.6 -- The 404 puzzle from the F4 audit

The audit reported "base host healthy but the two probed dataflow paths returned HTTP 404". Three reproducible causes, all fixable:

1. **Wrong dimension count.** `/USA?...` against `DSD_LFS@DF_IALFS_INDIC` returns HTTP 422 `"Not enough key values in query, expecting 9 got 1"`. The audit probably parsed this as 404 because some HTTP clients lump 4xx errors.
2. **Wrong dataflow ID.** `OECD.SDD.STES,DSD_STES@DF_VACAN_M` returns 404 because there is no such dataflow -- vacancies aren't published. Confirmed by listing all STES dataflows.
3. **Legacy host probe.** `stats.oecd.org/sdmx-json/data/...` returns HTTP 405 on HEAD and times out on GET. If the auditor probed both hosts in parallel, the legacy 405 may have been misread as 404.

None of these are intrinsic API problems. With the right key + dataflow + host, every probe returns 200.

---

## Phase 4 -- Integration plan for Nova

### Where it fits

Add alongside the existing geo-stats triad in `nova.py`:

```python
# nova.py:6588 area (tool routing)
"query_eurostat":   self._query_eurostat,
"query_uk_ons":     self._query_uk_ons,
"query_statcan":    self._query_statcan,
"query_oecd_sdmx":  self._query_oecd_sdmx,   # NEW
```

Implementation lives in `api_enrichment.py` (the existing pattern for `_query_eurostat`). The standalone sample at `docs/oecd_sdmx_sample.py` can be lifted into `api_enrichment.py` with two changes:
1. Rename `query_oecd` -> `fetch_oecd_sdmx_data` for consistency with siblings (`fetch_eurostat_data`, `fetch_uk_ons_data`).
2. Replace `urllib` with `requests` (rest of `api_enrichment.py` uses requests).

### Tool signature for the chatbot

```python
{
  "name": "query_oecd_sdmx",
  "description": (
    "Query OECD international labour-market statistics: monthly unemployment rates, "
    "labour force indicators, average wages, hours worked, productivity, and migration. "
    "Best for cross-country recruitment context across OECD countries "
    "(38 members + key partners). Use Eurostat for EU-specific data, UK ONS for UK, "
    "Statistics Canada for Canada, BLS for US-specific monthly data."
  ),
  "input_schema": {
    "type": "object",
    "properties": {
      "country": {
        "type": "string",
        "description": "ISO-3 country code, e.g. 'USA', 'GBR'. Multiple via '+': 'USA+GBR+DEU'.",
      },
      "dataset": {
        "type": "string",
        "enum": [
          "unemployment_rate_monthly",
          "labour_force_indicators",
          "average_annual_wages",
          "hours_worked",
          "productivity_levels",
          "migration_inflows",
        ],
      },
      "start_year": {"type": "string", "description": "e.g. 2024, '2025-01', '2025-Q1'"},
      "end_year": {"type": "string", "description": "e.g. 2026, '2026-04'"},
    },
    "required": ["country", "dataset"],
  },
}
```

### Cache strategy

OECD data is **monthly at best** (unemployment), more often quarterly or annual. There is no value in a sub-day TTL.

- **Cache key:** `oecd:{dataset}:{country}:{start}:{end}` (lowercased, sorted countries)
- **TTL:** **24 hours**
  - Monthly datasets refresh once per month with a 1-month lag; daily caching is sufficient
  - Annual datasets refresh once per year
- **Storage:** Reuse Nova's existing Supabase `cache` table (same as Eurostat, UK ONS, StatCan)
- **Cache miss handling:** Stale-while-revalidate -- serve stale up to 72h while async refresh

For very heavy dataflows (LFS_INDIC = 7 MB, MIG = 14 MB on broad queries), consider also caching at the **flattened-rows** layer rather than raw payload, since the flatten step is non-trivial.

### Error handling

The sample module already covers the three real error modes:

| HTTP | Cause | Handling |
|---|---|---|
| 200 + `NoRecordsFound` body | Combo of country + filter has no data | Return empty rows with explanatory note |
| 404 | Bad agency, dataflow, or version | Return error; do NOT retry |
| 406 | Missing/wrong Accept header | Should never happen if client uses constant Accept |
| 422 | Wrong dimension count in key | Catalogue bug -- return error and alert |
| Network timeout | Broad query without time bounds | Auto-narrow: retry with `lastNObservations=12` |

### Risks

| Risk | Severity | Mitigation |
|---|---|---|
| OECD changes URL pattern (third time in 8 years) | Medium | Centralise URL building in one helper; smoke test in CI |
| Data structure version bumps (e.g. `1.0` -> `2.0`) | Medium | Pin version in catalogue; fall back to `latest` on 404 |
| Dataset deprecation (e.g. `STLABOUR` removed) | Low | Keep catalogue per-dataset; degrade gracefully when one is missing |
| Payload size explosion (the 14 MB MIG case) | High for cost | Hard cap response to 5 MB; require country filter for MIG/EDU |
| No SLA, occasional outages | Low | Cache-first; UI shows "data from {timestamp}" |
| Confusing "obs limit" silent truncation | Medium | Always include `endPeriod`; warn in logs when row count == cap |
| Audit's 404 reading was wrong | (already happened) | Documented in Phase 3.6 |

### Effort estimate (revised down)

- Original budget: 4-6 days build + 2 day spike = 6-8 days
- Spike consumed: ~1 hour
- Revised build: **2.5 days** (1.5 day implementation, 0.5 day tests, 0.5 day docs)
- Optional follow-on: **+1.5 days** to expand catalogue from 6 to 15 datasets (add LFS by industry, education enrolment, working-age pop projections, etc.)

**Net saving vs original plan: 2-4 days.**

---

## Phase 5 -- Standalone sample

See `docs/oecd_sdmx_sample.py`. Self-contained, stdlib-only (urllib + json), runs as a script.

### Run the demo

```bash
cd /Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator
python3 docs/oecd_sdmx_sample.py
```

Live output (2026-05-22):
```
=== OECD SDMX demo: monthly unemployment for G5 (USA+GBR+DEU+FRA+JPN) ===
URL: https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_UNE_M,1.0/USA+GBR+DEU+FRA+JPN.UNE_LF_M.PT_LF_SUB._Z.Y._T.Y_GE15._Z.M?startPeriod=2026-01&endPeriod=2026-04
HTTP: 2714ms, 10352 bytes
Rows: 8
  Germany          2026-02   4.0  %
  United States    2026-03   4.3  %
  United States    2026-02   4.4  %
  United States    2026-01   4.3  %
  Japan            2026-02   2.6  %
  Japan            2026-01   2.7  %
  United Kingdom   2026-01   4.9  %
  France           2026-01   7.8  %

=== Bonus: average annual wages (USA, latest 4 years) ===
URL: https://sdmx.oecd.org/public/rest/data/OECD.ELS.SAE,DSD_EARNINGS@AV_AN_WAGE,1.0/USA......?startPeriod=2020&endPeriod=2023
Rows: 12
  United States  2020  82783.41  USD PPP
  United States  2021  84211.21  USD PPP
  United States  2022  81837.83  USD PPP
  United States  2023  82078.43  USD PPP
```

### Public API surface

```python
from docs.oecd_sdmx_sample import query_oecd, OECD_DATASETS

# What can I query?
print(list(OECD_DATASETS.keys()))
# -> ['unemployment_rate_monthly', 'labour_force_indicators', 'average_annual_wages',
#     'hours_worked', 'productivity_levels', 'migration_inflows']

# Single country, latest 3 months
r = query_oecd(country="USA", dataset="unemployment_rate_monthly",
               start_year="2026-01", end_year="2026-03")
# r["rows"] is a list of dicts: country, time_period, value, measure, unit, freq, dataset, source

# Multiple countries, annual data
r = query_oecd(country="USA+GBR+DEU", dataset="average_annual_wages",
               start_year=2020, end_year=2023)

# Error case: bad dataset name
r = query_oecd(country="USA", dataset="vacancy_rate")
# r["error"] = "Unknown dataset 'vacancy_rate'. Available: [...]"
```

### What the module does NOT do (yet)

These are deliberately out of scope for the spike. Address during real build:

1. **No Supabase caching** -- caller must wrap if needed
2. **No retry on transient 5xx** -- single attempt, fail fast
3. **No `requests` library** -- uses urllib so it's zero-dep for a spike
4. **No async** -- synchronous blocking call
5. **No streaming for large payloads** -- 14 MB MIG full pull is loaded into memory
6. **No automatic dimension discovery** -- catalogue is hand-curated (could be auto-generated from `/dataflow/.../?references=all` calls in a future iteration)

---

## Recommendation

**GO -- proceed to build in S51.** Allocate 2.5 days for the core integration. The audit's risk assessment ("might be deprecated") is incorrect for the new host; only the legacy stats.oecd.org is dying. The 6-dataset catalogue covers the main recruitment-intel use cases Joveo cares about, with a clear extension path.

If we want to add **vacancy rates** to Nova later, do NOT rely on OECD -- use Eurostat JVS (already integrated), BLS JOLTS (US, already integrated), and ONS (UK, already integrated). OECD is the right tool for cross-country macro context, wages, hours, productivity, and migration -- not vacancies.

---

## Appendix A -- Sources

- [OECD Data Explorer FAQ (2024-09)](https://www.oecd.org/en/data/insights/data-explainers/2024/09/OECD-DE-FAQ.html)
- [OECD SDMX API documentation page (2024-09)](https://www.oecd.org/en/data/insights/data-explainers/2024/09/api.html)
- [OECD Data API migration guide PDF (2024-07-22)](https://gitlab.algobank.oecd.org/public-documentation/dotstat-migration/-/raw/main/OECD_Data_API_documentation-Upgrading_from_the_legacy_OECD.Stat_APIs.pdf)
- [OECD Data Explorer (interactive UI)](https://data-explorer.oecd.org/)
- [OECD Employment Database](https://www.oecd.org/en/data/datasets/oecd-employment-database.html)
- [OECD Labour Force Statistics](https://www.oecd.org/en/publications/serials/oecd-labour-force-statistics_g1g1200b.html)
- [DBnomics catalogue of OECD dataflows](https://db.nomics.world/OECD)
- [SDMX-JSON 2.0 schema](https://raw.githubusercontent.com/sdmx-twg/sdmx-json/master/data-message/tools/schemas/2.0.0/sdmx-json-data-schema.json)
- [pandaSDMX Python library (for reference)](https://pandasdmx.readthedocs.io/)

## Appendix B -- All curl commands run in this spike

Save as `tmp/oecd_smoke_test.sh` to re-run anytime:

```bash
#!/usr/bin/env bash
set -euo pipefail
H='Accept: application/vnd.sdmx.data+csv; labels=both; charset=utf-8'
BASE='https://sdmx.oecd.org/public/rest/data'

echo '--- unemployment monthly (G5) ---'
curl -sSL -H "$H" "$BASE/OECD.SDD.TPS,DSD_LFS@DF_IALFS_UNE_M,1.0/USA+GBR+DEU+FRA+JPN._Z.Y._T.Y_GE15..M?startPeriod=2026-01&endPeriod=2026-04" \
  -w '\nHTTP:%{http_code} BYTES:%{size_download}\n' | head -3

echo '--- average annual wages (US/UK/DE) ---'
curl -sSL -H "$H" "$BASE/OECD.ELS.SAE,DSD_EARNINGS@AV_AN_WAGE,1.0/USA+GBR+DEU......?startPeriod=2020&endPeriod=2023" \
  -w '\nHTTP:%{http_code} BYTES:%{size_download}\n' | head -3

echo '--- labour force indicators (G5, single year) ---'
curl -sSL -H "$H" "$BASE/OECD.SDD.TPS,DSD_LFS@DF_IALFS_INDIC,1.0/USA+GBR+DEU.........?startPeriod=2024&endPeriod=2024" \
  -w '\nHTTP:%{http_code} BYTES:%{size_download}\n' | head -3

echo '--- productivity (G5, latest) ---'
curl -sSL -H "$H" "$BASE/OECD.SDD.TPS,DSD_PDB@DF_PDB_LV,1.0/USA+GBR+DEU+FRA+JPN........?lastNObservations=1" \
  -w '\nHTTP:%{http_code} BYTES:%{size_download}\n' | head -3

echo '--- migration (Finland, latest) ---'
curl -sSL -H "$H" "$BASE/OECD.ELS.IMD,DSD_MIG@DF_MIG,1.0/FIN.......?lastNObservations=1" \
  -w '\nHTTP:%{http_code} BYTES:%{size_download}\n' | head -3

echo '--- hours worked (Austria, latest) ---'
curl -sSL -H "$H" "$BASE/OECD.ELS.SAE,DSD_HW@DF_AVG_USL_WK_WKD,1.0/AUT............?lastNObservations=1" \
  -w '\nHTTP:%{http_code} BYTES:%{size_download}\n' | head -3

echo '--- legacy host check (expect timeout or 405) ---'
curl -sSI -m 10 'https://stats.oecd.org/sdmx-json/data/STLABOUR/USA.LRHUTTTT.STSA.M/all' \
  -w '\nHTTP:%{http_code}\n' | head -3
```
