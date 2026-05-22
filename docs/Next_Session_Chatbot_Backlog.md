# Nova Chatbot — Next Session Backlog (S80+)

**Session of record**: S76–S79b (2026-05-22). 9 commits live (`a83c504` → `51727cf`).
**Live version**: `4.0.0-51727cfe` on Render `srv-d6lk06k50q8c73bcpo40`.

## ✅ What's done (do NOT redo)

- Country awareness on CPA tools end-to-end (Tier 1+2)
- Tier 3 intelligence layer: planner, citations, number verifier, clarifier (env-flagged, default on)
- 750KB stranded healthcare data cluster wired into chat handlers
- `_query_kb_deep` fixed (was crashing on every call — unlocked 28 datasets)
- LinkUp/Revelio dead tool advertisements removed
- OECD SDMX integration (6 datasets verified live, `_query_oecd_sdmx` tool)
- RAG Phase 1 built (`_query_kb_semantic` tool, `rag_pipeline.py`, feature-flagged off)
- 65-country currency map + 47 currency symbols + multilingual CSS font fallback
- Linear MCP migrated from `/sse` to `/mcp`
- 503 regression tests pass
- Supabase: `nova_shared_conversations` table created, `cg_daily_raw` indexes added
- Enrichment scheduler re-enabled (`ENABLE_DATA_ENRICHMENT=1` on Render)
- Cache flush done

## 🔴 Real remaining issues (verified live, P1)

### 1. Berlin CPC still returns USD despite S79 country tagging
- **Symptom**: Query `"cpc for tech in berlin"` returns `$7.63` not `€` symbols.
- **Hypothesis**: My S79 fix added `country`/`currency`/`currency_symbol` fields to the tool result dict. But the LLM isn't honoring them — it sees the (USD-coded) `current_cpc` numbers from market_signals and renders with `$`.
- **Two-part fix needed**:
  1. **System prompt update**: Add a hard rule "When tool result includes `currency_symbol`, format ALL monetary figures with that symbol, NEVER with `$`."
  2. **Convert in handler**: Multiply USD numbers by `1/usd_rate` to get local currency, before returning. Today we tag with currency but pass through USD numbers.
- **Files to touch**: nova.py (get_system_prompt around line 3907, and _track_cpc around line 13065)

### 2. Tier 3 number-verifier markdown double-bold bug
- **Symptom**: `**4**5%**` visible in production responses (e.g. "CPCs surged **4**5%** above historical")
- **Root cause**: The number-verifier inserts `_[unverified]_` markers but splits the existing `**bold**` markdown when the number is inside bold. My S77 `_t3_adjust_offset_outside_bold` helper covers some cases but not all.
- **Fix**: Audit `_verify_response_numbers` in nova.py for all insertion sites. Should skip ENTIRE bold runs, not just adjust offset.
- **Test case**: Pre-compute on `"**45%**"` and `"**$1,250,000**"` — verifier should leave them alone OR insert tag AFTER the closing `**`.

### 3. `[unverified]` tags leak to user prose at 22% rate
- **Symptom**: QUAL-A eval: 4 of 18 responses had `_[unverified]_` visible to end users.
- **Two options** (pick one):
  - **A. Tune the verifier**: tighten the whitelist or widen the ±5% tolerance; tag fewer numbers
  - **B. Hide from rendered output**: keep tags in the LLM context for audit but strip them before returning to client (`response_text.replace("_[unverified]_", "")` in the final send)
- **Recommended**: B — preserves audit signal without UX leak

### 4. First message of a new session sometimes silently fails (handshake race)
- **Symptom** (VISUAL-A finding): Test #1 query immediately after page load returned no assistant response. Subsequent queries in same session worked.
- **Hypothesis**: WSGI worker cold-start or initial WebSocket handshake takes longer than client timeout
- **Investigation needed**: Check Render boot logs for the cold-start path; possible fix is a warmup ping or longer initial-message timeout

### 5. OECD SDMX queries take ~55s, hit 60s client timeout (QUAL-A Test 12)
- **Symptom**: Query "what's the labor force participation rate in germany 2026" times out
- **Root cause**: OECD endpoint latency 50-55s; chat client times out at 60s
- **Fix**: Either (a) cache OECD results more aggressively (24h cache already exists but isn't pre-warmed) OR (b) bump the chat tool-call timeout for `query_oecd_sdmx` specifically to 90s

## 🟡 Quality improvements (P2)

### 6. RAG Phase 2: backfill embeddings
- Phase 1 shipped in S78 with `RAG_V2_ENABLED=false`. Tool gracefully returns `rag_disabled`.
- Phase 2 work (per `docs/RAG_Design_2026.md`):
  - Run embedding backfill on all KB JSON files → Qdrant `nova_knowledge` collection
  - Verify Voyage AI key set (or fall back to sentence-transformers)
  - Set `RAG_V2_ENABLED=1` on Render
  - A/B test: 50-query eval comparing RAG-enabled vs RAG-disabled
- **Effort**: ~3 days

### 7. Tier 3 monitoring + observability
- Verify in production:
  - Planner is actually firing (check Sentry for `T3-1 planner` log lines)
  - Citation block appearing on 56%+ of responses (QUAL-A baseline)
  - Number verifier marking <20% as `[unverified]` (currently 22%)
- Add Slack alerts if any of these drift

### 8. 24h enrichment cron verification
- Set `ENABLE_DATA_ENRICHMENT=1` in S77. Scheduler runs 5 min after boot + hourly.
- Check `enrichment_log` in Supabase after 24h — should have ~24 rows
- Address pre-existing 55% failure rate (74/134 failed in local runs)

### 9. F3 content drift cleanup (decision deferred)
- F3-VERIFY found:
  - Glen Cathey 'Quality of Hire': URL is profile root, no specific post → remove or replace
  - Ben Eubanks 'Talent Scarcity': URL is homepage, no book ref → update URL
  - Tim Sackett 'AI screening inclusive': URL 404 → replace with Top 100 PDF
- All 3 still in `data/ta_leaders_curated_2026.json` with verification flag

## 🟢 Strategic upgrades (P3 — multi-day projects)

### 10. F1 deeper data confidence boost
- 27% of intl_role_benchmarks_v1.json cells are LOW confidence (extrapolated)
- Upgrade by sourcing primary government wage boards for those cells (~1-2 days)

### 11. F4 new API integrations (top-10 ranked)
1. OECD SDMX — DONE in S78
2. ESCO deep occupation taxonomy
3. Appcast Benchmark API
4. World Bank Jobs multi-indicator
5. talent.com Publisher
6. ABS Australia
7. Mapbox
8. Recruitics
9. Reddit JSON
10. Salary.com

### 12. Tier 3 planner cost/latency optimization
- Planner adds ~400ms per query
- Could batch with main tool call if Anthropic SDK supports
- Or skip planner for short queries (<20 chars)

## 📋 Operational pre-flight for new session

When the next Claude opens a Nova session:
1. Read `MEMORY.md` for S76/S77/S78 context
2. Run `python3 -m pytest tests/test_country_awareness.py tests/test_intl_chatbot_scenarios.py tests/test_currency_formatting.py tests/test_us_regression_safety.py -q` — should be 495+ pass
3. `curl https://media-plan-generator.onrender.com/api/health` — should be 200 healthy
4. `git log --oneline -10` — last commit should be `51727cf S79b`
5. Pick from this backlog by priority

## 🚨 What to NOT touch
- Tier 1 fast paths in nova.py (lines 14148, 15280, 16432) — already country-aware, 503 tests guard them
- `_query_kb_deep` at nova.py:7233 — the single-char fix from G3 is critical, don't revert
- `_NON_US_CITY_ALIASES` set — removing entries breaks intl detection
- `nova_shared_conversations` Supabase schema — production code writes a specific shape, don't change

## Critical files / line refs

- `nova.py` 26,762 lines after S79b
- `app.py` (login_log fix at /api/auth/session, deployed in S77)
- `rag_pipeline.py` 1,212 LOC (NEW in S78, standalone)
- `api_enrichment.py` (OECD SDMX function at line ~1100+)
- `tests/test_country_awareness.py` (495+ tests)
- `data/intl_role_benchmarks_v1.json` (505KB, 996 cited cells)
- `data/industry_reports_2026.json` (109KB, 80 reports)
- `data/ta_leaders_curated_2026.json` (108KB, 89 verified posts)

## Snapshots (rollback safety)
- `/tmp/nova.py.tier1_and_tier2_snapshot.py`
- `/tmp/nova.py.post_tier3.py`
- `/tmp/nova.py.live_snapshot.py`
- `~/.claude.json.s77_backup_20260522_170639` (Linear MCP backup)
