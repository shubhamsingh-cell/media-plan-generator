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

## Update 2026-07-31 (later same day): free metro-area substitute shipped, real vendor pricing checked

**Census CBSA (metro/micro area) shipped as a free substitute** — `data/geo/cbsa_by_county.tsv`,
built from the Census/OMB delineation file, joined onto the same `county_fips` every
location already resolves to. Every resolved US location now carries an optional
`cbsa_title` (e.g. "Houston-Pasadena-The Woodlands, TX"), surfaced in the wizard as
"Metro area (Census): …", labeled by source so it is never mistaken for a licensed
product. This is **not** a DMA equivalent — CBSA is a coarser, county-level Census
classification (1,915 counties join to a metro/micro area; rural counties legitimately
have none) — but it answers most of what clients mean by "metro-level targeting" today,
for $0 and with unambiguous public-domain provenance.

**Real-world DMA-alternative pricing, checked via public search — no vendor contacted,
no quote requested, nothing purchased:**

| Vendor | What it is | Public pricing found |
|---|---|---|
| ZipInfo.com (Melissa) "ZIP5 Market Area" | Its own proprietary 280-region taxonomy — **not** Nielsen's 210 DMAs, not TV/radio-viewership-based | Yes: $41.95–$104.95 one-time, or $251.95–$628.95/yr enterprise |
| Claritas PRIZM | Consumer lifestyle segmentation — not a county/ZIP→market-area crosswalk, doesn't substitute for DMA geography | Yes: $109–199/report, or "from $1,645/yr" |
| comScore Markets | The real Nielsen-DMA equivalent — 210 ZIP-based local markets; this is what Meta is adopting as its own DMA replacement | **No public price.** Contact-sales only; license terms bar resale/redistribution |
| Neustar/TransUnion geo APIs | — | No public pricing found anywhere |
| Alteryx geocoder/market-area add-ons | — | Gated behind a Designer license + sales page; no public figure |

**Conclusion: nothing that is actually DMA-equivalent has public pricing.** The two
products with a real, self-serve price (ZipInfo, Claritas) are both adjacent products
with different region definitions, not substitutes for DMA geography. comScore Markets —
the one product that genuinely replaces Nielsen DMA — is enterprise-contract-only with
no disclosed floor. This confirms the standing conclusion below: Census CBSA remains the
only verified sub-$5k, non-proprietary option, and it is now shipped.

## The decision to take to legal/procurement

1. Do we want to license Nielsen DMA definitions (or the comScore Markets equivalent) for
   use in client-facing media plans? Both are commercial, contact-sales-only products;
   we have not requested pricing and this document does not authorize doing so.
2. Given free Census CBSA now covers the metro-grouping need for most clients (shipped
   2026-07-31), is a paid DMA/comScore license still worth pursuing, or does CBSA close
   the gap well enough that this stays parked indefinitely?
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
> US Census data, and shows you exactly what it understood before the plan is built —
> including the Census metro area (CBSA) when one applies, e.g. "Houston-Pasadena-The
> Woodlands, TX." True Nielsen DMA targeting isn't in yet — DMA definitions are a
> licensed Nielsen product, and we're working through that properly rather than shipping
> an approximation. If your team already licenses DMA data, we can look at ingesting
> yours directly.
