# Bundle Quality Fix Plan — 2026-07-03 (execute after 4pm IST)

**Owner approval gates:** show before/after renders BEFORE any push. `main` auto-deploys on Render — NO push to main without explicit owner OK. Commit and push as SEPARATE commands (chained `git commit && git push` gets the whole command denied).

## Context (self-contained — safe for a fresh session)

A 2026-07-03 multi-agent review of two generated bundles (Pratt & Whitney New Zealand, Jul 2 = post-reskin; Omada.ai, Jun 16 = pre-reskin) confirmed **78 major/critical findings + 38 minors, 0 refuted**. Full findings with exact slide/cell refs: `BUNDLE_QC_FINDINGS_2026-07-03.json` (repo root). They collapse into ~10 root-cause fixes below.

Reference bundles for regression comparison (may be cleaned; regenerate if missing):
- `/private/tmp/claude-501/-Users-shubhamsinghchandel-untitled-folder/4963b87e-f7f1-4f74-9f7f-5cd710a3c58d/scratchpad/review/{pratt,omada}/` (unzipped bundles + `slides/*.png` renders)
- Originals: `~/Downloads/Pratt_Whitney_New_Zealand_Media_Plan_Bundle.zip`, `~/Downloads/Omada_ai_Media_Plan_Bundle (1).zip`

## Git strategy

1. `git -C "/Users/shubhamsinghchandel/untitled folder/media-plan-generator" fetch origin`
2. Create ONE worktree for the whole effort off origin/main: branch `fix/bundle-quality-2026-07`. Do NOT touch the existing checkout (it sits on `deck-invisible-reskin` with a dirty tree; a concurrent agent may be using it).
3. Sonnet agents work inside that worktree on DISJOINT files/functions (see assignments) — sequence any two tasks that touch the same function.
4. Commit per-workstream with clear messages. Push branch (never main) only after verification gates pass; then show owner before/after and await approval to merge.

## Model routing (orchestrator does this via Agent/Workflow `model:` param — no /model needed)

- `model: 'sonnet'` → S1–S6 (mechanical, well-specified)
- `model: 'opus'` → O1–O3 (layout geometry engine, cross-artifact design decision, final review)

## SONNET 5 workstreams (parallelizable; S2/S3/S5 all touch excel_v2.py — sequence or split by function)

### S1 — Internal QA leak gating (ppt_generator.py + excel_v2.py + app.py)
- Red "Creative QC: 45/100 — Grade F" badge renders on exec-summary slide of BOTH decks; cover carries "Caution: Minimal data available. Plan is heavily estimated."; Excel Exec Summary B72 "Overall: 45/100 (Grade F)" + Sources & Confidence B3 grade "D"; validation banner "7 checks run | 7 passed" (false given known defects).
- Fix: single flag (env `NOVA_INTERNAL_QC=1` or request param) gates ALL internal QC/caution/grade artifacts; default OFF for client bundles. Investigate why Creative QC returns identical 45/100 for two different plans — if scorer is degenerate, log it, don't render it.

### S2 — Excel Total-row column shift (excel_v2.py)
- Exec Summary Total row formulas offset one column left: `D28=SUM(C22:C27)` sums the % column into Amount → renders "$1" as total (both workbooks; Pratt row 30, Omada row 28).
- Fix at the `_write_table_row`/totals-writer call site; add regression test asserting Total-row formula ranges align with their own column letters.

### S3 — Currency propagation (excel_v2.py + ppt_generator.py, uses plan_currency.py)
- Deck headline uses NZ$ but slides 5/7/9 revert to bare $; ENTIRE workbook is USD — literal "Budget (USD)" header on an NZ plan; Intl Benchmarks sheet all USD.
- Fix: thread the plan's currency (already computed by `plan_currency.py`) through excel_v2 headers/number_formats and the remaining ~30 hardcoded `$` sites in ppt_generator. Benchmarks that are genuinely US-calibrated may stay USD but MUST be labeled "(USD, US-calibrated)" — never bare $ beside NZ$ figures.

### S4 — Localization + no-data guards (data_synthesizer.py / kb sections / ppt_generator.py / excel_v2.py)
- NZ plan shipped: US-only niche boards (ClearedJobs.Net, ClearanceJobs, Military.com, USAJOBS, Hire Heroes USA), US security-clearance framework, `Country = "United States"` in Market Intelligence, US metros.
- Deck claims "Live Postings: 75,000 active jobs" (Pratt) / "180,000" (Omada) while the workbook's own MARKET DEMAND row records Postings=0 — suppress any "Live"-labeled stat whose source field is 0/empty.
- "White Collar dominant (100%)" printed for Aircraft Tradesperson — only print collar profile when classifier returns real confidence; fix trades misclassification if cheap.
- Fix: country-aware filters (non-US → drop US-only boards/clearance/metros; round-trip actual country), plus a generic "no fabricated stats over empty data" guard.

### S5 — Excel craft pass (excel_v2.py + upstream data hygiene)
- ~50% of money/percent values are text strings → numeric + number_format (extend S89 `_write_num` pattern; the findings JSON lists exact ranges).
- 120-char mid-word truncation (e.g. Channels & Strategy G16/G18) → wrap into taller rows, never truncate.
- Raw snake_case dict dumps (Market Intelligence D70:D114, up to 4,215 chars/cell) → curated key:value tables or drop.
- snake_case channel ids leak client-facing (Channels & Strategy B4:B9, deck slide 10) → display-name map.
- "pheonix" ×6 + lowercase city names → fix at data source.
- Degenerate salary rows (Min=Median=Max=$95K; P25 < Min) → suppress range or fix synthesizer.
- Benchmark "beating" logic: tie ≠ beating (deck s09 green arrows on equal values).
- Zero-budget TEST & LEARN rows projecting apps/hires → zero out projections.
- Boilerplate dedupe: identical "(National) Amazon, Walmart, UPS, FedEx" ×10 city rows; 3 identical competitor cards → dedupe or drop section when no differentiated data.
- Gridlines: set `showGridLines=0` on all sheets; extend canvas fill; brand tab color for Intl Benchmarks sheet.

### S6 — Slide-6 channel table foots (ppt_generator.py)
- Table shows 6 of 8 channels but prints 8-channel totals (rows sum NZ$142.5K vs printed NZ$150K); percentages print "101%".
- Fix: fit-aware — if rows exceed space, aggregate the tail into a labeled "+N smaller channels — NZ$X" row so the table always foots; largest-remainder rounding so percentages sum to exactly 100%.

## OPUS 4.8 workstreams (sequential, after or alongside S-fleet on disjoint files)

### O1 — Exec-summary fit-aware layout engine (ppt_generator.py, slide-2 builder + slide-5 chart)
- Headline clips mid-word behind cards (both decks); Situation-card Salary Range row overflows card bottom; KPI band wraps "NZ$33.95" onto two lines; slide-5 "CHANNEL CATEGORY ATTRIBUTION" header collides with 8th bar row.
- Fix: measure-then-place — 2-line clamp + autoshrink for headline; card content clamps; KPI value autoshrink keyed to string length (NZ$ prefixes); bar-chart row-count-aware section spacing. MUST verify by render loop (below), not just geometry math. Do not regress `tests/test_deck_layout.py` (38 tests).

### O2 — Two-plans reconciliation + goal-gap statement (app.py + excel_v2.py + budget_engine.py)
- Channel Recommendations sheet is a second contradictory plan (Pratt: 1,637 apps @ $91.63 vs main 4,418 @ $33.95; Omada: 4,882 @ $49.16 vs 12,755 @ $19; spend sums $240,240 ≠ $240,000).
- Decide: derive that sheet from the SAME budget_engine allocation (preferred) or reframe as explicit "Alternative scenario" with a reconciliation note. Also: hire goal vs projection must be addressed head-on (Omada: goal 5,000 vs projected 143) — add a goal-gap callout, never silently ignore. Duration contradictions (1-2 years vs 12-week plan) → single source of truth.

### O3 — Final verification + before/after for owner
- Regenerate BOTH bundles (NZ worst-case + US case) via `generate_pptx`/`generate_excel_v2` (see scripts/render_sample_outputs.py), run all gates, produce side-by-side before/after PNGs, write summary for owner approval. NO push to main.

## Verification gates (every workstream, run inside the worktree)

1. `python3 -m pytest tests/ -x -q` (esp. tests/test_deck_layout.py).
2. Regenerate sample bundle(s); NZ + US variants.
3. Deck render loop: Keynote AppleScript PPTX→PDF (open, wait `count of documents > 0`, export, close) → PyMuPDF PNGs → geometry analyzer (python-pptx: off-slide >1% bleed, text-overlap >30%, sub-8pt) → target 0 issues; then EYEBALL the PNGs.
4. Workbook assertion script: totals foot to stated totals; money cells numeric with formats; no "N/A"; no "pheonix"; currency symbol consistent with plan country; no text >120 chars truncated mid-word.
5. Diff check vs findings JSON: every P0/P1 finding either fixed or explicitly deferred with reason.

## Sequencing after 4pm

1. Worktree setup (5 min)
2. S-fleet in parallel where files are disjoint: S1+S4+S6 together; S2→S3→S5 sequenced on excel_v2.py (or split by function after quick collision check) — model: sonnet
3. O1 in parallel on ppt_generator.py slide-2/5 builders (coordinate with S6's slide-6 edits — different builders, same file: sequence commits) — model: opus
4. O2 after S2/S3 land — model: opus
5. O3 final gate — model: opus
6. Present before/after to owner; await approval; merge/push only on OK.
