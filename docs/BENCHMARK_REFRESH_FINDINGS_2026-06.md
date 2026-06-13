# #8 Benchmark Refresh — Investigation Findings (2026-06-13)

**Status:** 🛑 BLOCKED / RE-SCOPED. The task as written —
"QStash cron re-aggregating `cg_daily_raw` → `cg_benchmarks`" — **cannot be
safely built inside media-plan-generator.** Doing it blind would corrupt the
S89 keystone data. Details below.

## What I verified (live, Supabase `trpynqjatlhatxpzrvgt`, read-only)

| Table | Rows | Notes |
|---|---|---|
| `cg_benchmarks` | 6,175 | 2 clients (`default`, `ThreeHyphen`), 72 titles, 1 period (`all_time`), `last_updated` 2026‑05‑12 |
| `cg_daily_raw` | 522,775 | 13,659 `post_id`s, `date` ≤ 2026‑05‑11 |

**`cg_benchmarks` columns:** id, **client_name**, location, title, category,
day_of_week, **avg_nr, avg_gr, avg_profit_pct**, avg_applies, avg_cost,
**avg_multiplier**, sample_size, total_runs, period, first_seen, last_updated.

**`cg_daily_raw` columns:** id, session_id, upload_id, post_id, date, location,
title, category, template_type, media_cost, impressions_cumul, clicks_cumul,
applies_cumul, daily_impressions, daily_clicks, daily_applies, day_num,
created_at.

## Why a re-aggregation here is unsafe

1. **Columns that don't exist in the raw table.** `cg_benchmarks` carries
   `client_name` and the revenue economics `avg_nr` (net revenue), `avg_gr`
   (gross revenue), `avg_profit_pct`, and `avg_multiplier`. **None of these
   exist in `cg_daily_raw`** (no client, no revenue, no multiplier columns). An
   aggregation from raw can recompute only cost / applies / sample_size /
   total_runs / day_of_week — it would have to **drop or fabricate** the rest.

2. **The revenue model is not a simple formula.** Observed relationships:
   `avg_nr = avg_gr − avg_cost`; `avg_profit_pct = avg_nr/avg_cost × 100`. But
   `avg_gr` is **not** `applies × multiplier` and not a constant per-apply value
   — `avg_gr/avg_applies` ranges across rows (1.2, 1.6, 11.0, …) and
   `avg_multiplier` (1.0–11.0) is an independent per-row input. The true
   gross-revenue derivation depends on inputs not present in this repo or in
   `cg_daily_raw`.

3. **No in-repo population logic.** Nothing in `scripts/` or anywhere in the
   repo populates `cg_benchmarks`. The companion tables — `cg_upload_history`,
   `cg_sessions`, `cg_jobs`, `cg_schedules`, `cg_action_plans` — show the
   warehouse is owned by a **separate ingestion/scheduling pipeline** (the
   `cg_*` "Campaign Genius"-style uploader), which is where the
   upload → benchmark transform (incl. the revenue model + client mapping)
   actually lives.

4. **media-plan-generator is a pure READER.** `supabase_data.py` is GET-only on
   the **anon** key (no write path). `get_real_outcomes()` reads `cg_benchmarks`
   as first-party measured truth and feeds it into live plans. Overwriting that
   table from an incomplete aggregation would silently degrade every plan that
   matches a warehouse title.

## Re-scoped recommendation

The benchmark refresh is **not MPG's job to compute.** Two safe paths — needs a
product decision (see SESSION_HANDOFF §6):

- **(A) Trigger, don't compute (preferred).** The owning `cg_*` uploader
  pipeline already knows the transform. Have the QStash cron call **that
  system's** refresh entrypoint (endpoint or Supabase RPC it owns), or schedule
  it there. MPG keeps only its existing `/api/cron/run` + `get_real_outcomes()`
  reader. **Need from user:** which system owns the upload→benchmark transform
  and its refresh entrypoint.

- **(B) Canonical RPC in Postgres.** If the transform can be expressed in SQL,
  put it in a `SECURITY DEFINER` function `refresh_cg_benchmarks()` (migration,
  applied once) that MPG's cron calls via PostgREST RPC — *no app write creds
  needed*. **Blocker:** the exact gross-revenue / multiplier / client-mapping
  logic must come from whoever built the current `cg_benchmarks` rows; it is not
  reconstructable from `cg_daily_raw` alone.

**Do NOT** ship a cost/applies-only re-aggregation — it corrupts the revenue
columns and `client_name`. The infra (`/api/cron/run`, `CRON_SECRET`) is already
in place from S88; only the *correct, owned* refresh action is missing.
