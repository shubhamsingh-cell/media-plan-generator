-- Shared alert-cooldown store for the monitoring alert bridge (S90 P1).
-- See docs/INCIDENT_2026-06-13_alert_noise.md and alert_cooldown_store.py.
--
-- Run once in the Supabase SQL editor (or via the CLI). Until this table
-- exists AND SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY are set, the alert
-- bridge transparently falls back to the previous in-memory cooldown.

create table if not exists public.alert_cooldowns (
    alert_key     text primary key,
    last_fired_ts double precision not null,
    updated_at    timestamptz      not null default now()
);

-- The bridge upserts via PostgREST with `Prefer: resolution=merge-duplicates`,
-- which relies on the primary key above to merge on conflict.

-- Service-role key is used server-side; no row-level-security policy is needed
-- for the bridge. If RLS is enabled on this schema, add a policy that allows
-- the service role full access, e.g.:
--   alter table public.alert_cooldowns enable row level security;
--   create policy "service role full access" on public.alert_cooldowns
--     for all to service_role using (true) with check (true);

-- ---------------------------------------------------------------------------
-- Atomic claim RPC (closes the cross-worker read-then-write race).
-- The bridge calls this via POST /rest/v1/rpc/claim_alert_cooldown. It does the
-- check-and-set in ONE statement under a row lock: returns TRUE when the alert
-- is claimed (-> fire) and the row is (re)stamped, FALSE when still cooling
-- down (-> suppress). A missing/future/corrupt timestamp is treated as
-- claimable (fail-open). If this function is absent the RPC errors and the
-- bridge falls back to read-then-write, so it is optional but recommended.
create or replace function public.claim_alert_cooldown(
    p_key      text,
    p_now      double precision,
    p_cooldown double precision
) returns boolean
language sql
as $$
    with upsert as (
        insert into public.alert_cooldowns as ac (alert_key, last_fired_ts, updated_at)
        values (p_key, p_now, now())
        on conflict (alert_key) do update
            set last_fired_ts = excluded.last_fired_ts,
                updated_at    = now()
            -- Re-claim only if the prior fire is stale, or a future/corrupt
            -- timestamp (ac.last_fired_ts > p_now) that must never suppress.
            where ac.last_fired_ts > p_now
               or (p_now - ac.last_fired_ts) >= p_cooldown
        returning 1
    )
    select exists (select 1 from upsert);
$$;
