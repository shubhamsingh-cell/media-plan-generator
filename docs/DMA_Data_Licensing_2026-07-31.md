# DMA Data Licensing — decision brief

**Date:** 2026-07-31
**Owner:** Shubham Singh Chandel
**Status:** OPEN — needs a legal/procurement call before any DMA data ships
**Trigger:** Jesse Ofner asked whether Nova uses Nielsen DMA data (2026-07-31)

---

## The ask

A client asked whether our media plans are DMA-aware. Today they are not — Nova had no
geographic resolution at all before this change, and the location-resolution work shipped
alongside this brief deliberately ships **without** DMA.

## Why DMA is different from every other geo field we just shipped

City, county, state, ZIP and lat/lng all come from US Census Bureau files, which are
US-government works in the public domain. We can bundle and redistribute them in a
commercial product with no permission and no fee.

DMA is not like that. "Designated Market Area" is a **Nielsen commercial product** — a
proprietary map of 210 US television markets that Nielsen defines, maintains, and
licenses. There is no federal or public-domain equivalent. The FCC references DMAs in
rulemaking but does not publish a clean crosswalk file.

## What we found when we looked for a free source

Verified by direct download on 2026-07-31:

| Source | What it has | License |
|---|---|---|
| `alex-patton/US-TVDMA-BY-COUNTY` (GitHub) | 3,149 rows, county FIPS → DMA **name**. No DMA codes. ~7 Virginia/Baltimore counties carry wrong FIPS. | **None stated.** GitHub API reports `license: null`. README says "use at your own risk." |
| `fissehab/Nielsen-Media-Research-DMA` (GitHub) | 210 rows, DMA name → DMA **code**. No counties. | **None stated.** `license: null`. |
| Kaggle county-DMA-FIPS mapping | Not evaluated | Requires login/API key — fails the "freely downloadable" bar |
| FCC.gov | No clean crosswalk found; DMA references appear only inside PDF filings | n/a |

Every free crosswalk that exists is somebody's **unlicensed derivation of Nielsen's
proprietary map**, republished without a license grant. "Publicly available on GitHub"
is not the same as "licensed for redistribution" — under GitHub's default terms, a repo
with no license permits viewing and forking, not commercial redistribution.

To ship DMA from these sources we would also have to patch bad FIPS rows by hand and
join two separately-unlicensed datasets by fuzzy name matching. So the data quality is
mediocre *and* the provenance is unclear — a bad combination for a field that would
appear in client-facing media plans and exported decks.

## Recommendation

**Do not bundle a derived DMA crosswalk.** Ship the Census-based resolution now, answer
Jesse honestly ("not yet — we're sourcing licensed DMA data"), and take the licensing
question to legal/procurement.

The engineering cost of adding DMA later is near zero. `plan_location.py` already reads
an optional `data/geo/dma_by_county.tsv` (columns: `county_fips`, `dma_code`,
`dma_name`) and populates DMA fields when that file is present. Whenever a licensed
source lands, DMA becomes a **data-only commit** — no code change, no schema change, no
redeploy of the resolver logic.

## The decision to take to legal/procurement

1. Do we want to license Nielsen DMA definitions for use in client-facing media plans?
   Nielsen licenses this data commercially; we have not requested pricing.
2. If not, is there an alternative market taxonomy we already have rights to — for
   example the Census CBSA/metro-area definitions, which are public domain and cover
   substantially the same planning need for most clients?
3. If a client supplies their own licensed DMA list under their Nielsen contract, may we
   ingest it per-client rather than bundling one globally?

Option 3 is worth flagging: several large agency clients already hold Nielsen licenses,
and per-client ingestion sidesteps our own licensing need entirely.

## What NOT to do

- Do not label any derived data as official Nielsen data, in the product, in code, in
  comments, or in a client conversation.
- Do not bundle a crosswalk on the reasoning that "it's on GitHub so it's free."
- Do not claim DMA support in a deck, demo, or sales conversation before this closes.

## Interim answer for Jesse

> Nova now resolves every location you enter down to city, county, state and ZIP against
> US Census data, and shows you exactly what it understood before the plan is built.
> DMA-level targeting isn't in yet — DMA definitions are licensed from Nielsen, and
> we're working through that properly rather than shipping an approximation. If your
> team already licenses DMA data, we can look at ingesting yours directly.
