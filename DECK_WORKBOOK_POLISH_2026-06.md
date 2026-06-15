# Deck + Workbook Baseline Polish — June 2026

Conformed the generated **PPTX** to the Joveo *"Invisible Media Planning
Approach"* reference deck and fixed the **XLSX** output. Branch:
`deck-baseline-polish` (committed locally, **not pushed**).

## Result

| Artifact | Before | After |
|---|---|---|
| Deck layout defects (worst-case Uber payload) | **92** (42 overlap, 31 sub-8pt, 19 off-slide) | **0** functional (2 flags are intentional full-bleed cover accents) |
| Deck — verified across 4 payloads (US multi-loc, UK intl, AI-training, minimal), 41 slides | — | **0** off-slide text, **0** overlaps |
| Workbook raw/unformatted numbers | 17 | **0** |
| Workbook unstyled Calibri cells | many | **0** |
| Font rendering | Times **serif** substitute (Poppins not embedded) | **Poppins embedded** → on-brand everywhere |
| Currency on US plans | `Chicago, IL` → **ILS (₪)** | **USD** |
| Tests | 201 | **239 pass** (incl. new layout regression suite) |

Fixed comparison files written next to the originals:
`~/Downloads/Uber_Media_Plan_Bundle/Uber_Strategy_Deck_FIXED.pptx` and
`…_Media_Plan_FIXED.xlsx`.

## Root causes & fixes

### PPTX — `ppt_generator.py`
- **Footer system (every slide):** three functions (`_add_footer`,
  `_add_data_sources_footnote`, `_add_enrichment_badge`) all drew text at
  y≈7.0–7.3in → ~15 overlaps + ~31 6–7pt lines. Now **one** 9pt footer band;
  the badge is a no-op; disclaimers sit above the footer, truncated to fit.
- **Exec summary KPI strip:** fixed 1.9in step marched the 6th/7th KPI 0.7–2.6in
  **off-canvas**. Now capped to 5 and sized to the available width.
- **Creative-QC badge:** overlapped the client name in the header band 95%.
  Moved to the top-right content corner.
- **Channel-strategy benchmark table:** unbounded (13+ rows) ran off the bottom
  and overlapped the attribution cards. Capped to what fits above the band
  (~5 rows, matching the reference deck).
- **Budget slide:** an embedded pie was dropped at a fixed spot over the table,
  callout and footer. Removed; the ROI insight is now a **dark-indigo takeaway
  band** (reference-deck style) positioned below the totals row.
- **Quality-outcomes fallback:** a hero card overlapped the projections row
  (removed); the insight callout now sits below the actual table bottom.
- **Global:** 8pt readability floor in `_set_font`; em-dashes; comparison
  legend/header separated.
- **Font embedding:** `_embed_fonts_in_pptx` injects Poppins (regular+bold) into
  the .pptx (OOXML surgery; python-pptx has no API for it).

### XLSX — `excel_v2.py`
- The pie chart's source-data table (J:K) was written raw → stray **Calibri +
  unformatted numbers**. Now styled and the helper columns are **hidden**.
- Channel Recommendations clicks/apps/hires now use `#,##0` number format.
- Em-dashes.
- Note: the "truncation" my first pass flagged was **not** data loss — those
  cells use `wrap_text` with auto-fit row height, so text is fully visible.

### `plan_currency.py`
- A trailing 2-letter **US state code** is now treated as USD (it was matched as
  an ISO country code: IL→Israel, CA→Canada, …). Fixes wrong currency symbols
  in both the deck and the workbook.

### `tests/test_deck_layout.py` (new)
- Locks in: text on-slide bounds, the 8pt floor, and font embedding, against a
  content-heavy worst-case payload.

## How to verify
```bash
python3 ppt_generator.py                       # build sample deck
python3 -m pytest tests/test_deck_layout.py \
  tests/test_ppt_polish.py tests/test_excel_provenance.py \
  tests/test_plan_currency.py -q               # 239 pass
```

## Open product decisions (need your call — not changed)
1. **Creative-QC "Grade F" badge** still shows a red `45/100 / Grade F` chip on
   the client-facing exec summary. Geometry is fixed; whether to soften/suppress
   low scores in a *client* deck is your call.
2. **Publisher-count copy** is inconsistent ("10,238+ publishers" vs "91+ job
   board platforms"). Pick one canonical number.
3. **XLSX narrative columns** wrap to several lines on a few sheets. Readable,
   but widening specific rationale/description columns would look cleaner — risky
   to auto-tune because columns are shared across multiple tables per sheet.
