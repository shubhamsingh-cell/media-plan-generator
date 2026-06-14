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
