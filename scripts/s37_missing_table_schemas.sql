-- ═══════════════════════════════════════════════════════════════════════════════
-- S37: Disaster Recovery Schemas for 4 Manually-Created Tables
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- These tables were created manually in Supabase but had no schema file.
-- Run this in Supabase SQL Editor to recreate them from scratch if needed.
--
-- Tables:
--   1. nova_memory        -- Nova AI persistent memory (conversations, facts, prefs)
--   2. plan_events        -- Durable log of every generated media plan
--   3. enrichment_log     -- Audit trail for data enrichment runs
--   4. metrics_snapshot   -- Row-per-metric counters surviving deploys
--
-- Created: 2026-04-03
-- Source: Reverse-engineered from codebase + live Supabase inspection
-- ═══════════════════════════════════════════════════════════════════════════════


-- ─── TABLE 1: nova_memory ────────────────────────────────────────────────────
-- Used by: nova_memory.py (NovaMemory class)
-- Stores conversation summaries, learned facts, and user preferences.
-- Code writes: user_id, content, memory_type, metadata
-- Code reads:  id, user_id, content, memory_type, metadata, created_at
-- Also has: key, value, updated_at, expires_at (from migration 003)

CREATE TABLE IF NOT EXISTS nova_memory (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     TEXT        NOT NULL,
    memory_type TEXT        NOT NULL CHECK (memory_type IN ('short_term', 'long_term', 'preference')),
    key         TEXT        NOT NULL DEFAULT '',
    value       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    content     TEXT        NOT NULL DEFAULT '',
    metadata    JSONB       DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now(),
    expires_at  TIMESTAMPTZ
);

-- Index: user lookups (load() filters by user_id)
CREATE INDEX IF NOT EXISTS idx_nova_memory_user
    ON nova_memory(user_id);

-- Index: user + type filtering (load() filters by memory_type)
CREATE INDEX IF NOT EXISTS idx_nova_memory_type
    ON nova_memory(user_id, memory_type);

-- Index: TTL-based cleanup of expired entries
CREATE INDEX IF NOT EXISTS idx_nova_memory_expires
    ON nova_memory(expires_at) WHERE expires_at IS NOT NULL;

-- RLS
ALTER TABLE nova_memory ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all on nova_memory" ON nova_memory
    FOR ALL USING (true) WITH CHECK (true);

-- Auto-update updated_at on row modification
CREATE OR REPLACE FUNCTION update_nova_memory_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER nova_memory_updated_at
    BEFORE UPDATE ON nova_memory
    FOR EACH ROW
    EXECUTE FUNCTION update_nova_memory_updated_at();


-- ─── TABLE 2: plan_events ────────────────────────────────────────────────────
-- Used by: app.py (_log_plan_event) for plan generation audit log
-- Also used by: plan_events.py (EventStore._persist_event) for event sourcing
--
-- NOTE: The s37_create_tables.sql already has this schema. This is a
-- verified copy for disaster recovery completeness.
--
-- app.py writes: event_type, plan_id, client_name, industry, budget,
--   roles, locations, channels_selected, user_email, user_name,
--   generation_time_ms, file_size_bytes, error_message, enrichment_apis, metadata
-- plan_events.py writes: event_id, plan_id, event_type, timestamp,
--   user_id, payload, version (via event.to_dict())

CREATE TABLE IF NOT EXISTS plan_events (
    id                  UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type          TEXT    NOT NULL DEFAULT 'plan_generated',
    plan_id             TEXT,
    client_name         TEXT,
    industry            TEXT,
    budget              TEXT,
    roles               JSONB   DEFAULT '[]',
    locations           JSONB   DEFAULT '[]',
    channels_selected   JSONB   DEFAULT '{}',
    channel_count       INTEGER DEFAULT 0,
    user_email          TEXT,
    user_name           TEXT,
    generation_time_ms  FLOAT,
    file_size_bytes     INTEGER,
    error_message       TEXT,
    enrichment_apis     JSONB   DEFAULT '[]',
    metadata            JSONB   DEFAULT '{}',
    created_at          TIMESTAMPTZ DEFAULT now(),
    -- Event-sourcing columns (plan_events.py EventStore)
    event_id            TEXT,
    "timestamp"         TEXT,
    user_id             TEXT,
    payload             JSONB,
    version             INTEGER
);

CREATE INDEX IF NOT EXISTS idx_plan_events_created
    ON plan_events(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_plan_events_client
    ON plan_events(client_name);

CREATE INDEX IF NOT EXISTS idx_plan_events_user
    ON plan_events(user_email);

CREATE INDEX IF NOT EXISTS idx_plan_events_plan_id
    ON plan_events(plan_id);

-- RLS
ALTER TABLE plan_events ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all on plan_events" ON plan_events
    FOR ALL USING (true) WITH CHECK (true);


-- ─── TABLE 3: enrichment_log ─────────────────────────────────────────────────
-- Used by: data_enrichment.py (_save_enrichment_log_to_supabase, _load_state_from_supabase)
-- Defined in: supabase_schema.sql (authoritative -- confirmed via live column check)
--
-- Code writes: action='refresh', source, records_affected, details (JSON string)
-- Code reads:  source, records_affected, details, created_at
-- Schema has:  table_name (NOT NULL), but code omits it -- defaults needed
--
-- NOTE: create_missing_tables.sql had a DIFFERENT schema (source, started_at,
-- success, records, metadata) that does NOT match production. The live table
-- uses the supabase_schema.sql definition below.

CREATE TABLE IF NOT EXISTS enrichment_log (
    id                BIGSERIAL   PRIMARY KEY,
    table_name        TEXT        NOT NULL DEFAULT '',
    action            TEXT        NOT NULL,
    records_affected  INTEGER     DEFAULT 0,
    source            TEXT,
    details           JSONB       DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_enrichment_table
    ON enrichment_log(table_name);

CREATE INDEX IF NOT EXISTS idx_enrichment_date
    ON enrichment_log(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_enrichment_source
    ON enrichment_log(source);

-- RLS
ALTER TABLE enrichment_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all on enrichment_log" ON enrichment_log
    FOR ALL USING (true) WITH CHECK (true);

-- Cleanup function: remove entries older than 30 days
CREATE OR REPLACE FUNCTION cleanup_old_enrichment_logs() RETURNS void AS $$
BEGIN
    DELETE FROM enrichment_log WHERE created_at < now() - interval '30 days';
END;
$$ LANGUAGE plpgsql;


-- ─── TABLE 4: metrics_snapshot ───────────────────────────────────────────────
-- Used by: monitoring.py (SupabasePersistence class)
-- Row-per-metric schema: each counter is a separate row keyed by metric_key.
--
-- Code writes: metric_key, metric_value, updated_at (via POST with merge-duplicates)
-- Code reads:  metric_key, metric_value
-- morning_brief.py reads: total_plans, active_conversations, avg_latency_ms,
--   error_rate, healthy_providers (these are metric_key values, not columns)
--
-- The monitoring.py comment says:
--   CREATE TABLE IF NOT EXISTS metrics_snapshot (
--       id TEXT PRIMARY KEY DEFAULT 'singleton',
--       data JSONB NOT NULL,
--       updated_at TIMESTAMPTZ DEFAULT now()
--   );
-- But the ACTUAL live schema uses row-per-metric (metric_key as PK).

CREATE TABLE IF NOT EXISTS metrics_snapshot (
    metric_key      TEXT        PRIMARY KEY,
    metric_value    INTEGER     NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_metrics_snapshot_updated
    ON metrics_snapshot(updated_at DESC);

-- RLS
ALTER TABLE metrics_snapshot ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all on metrics_snapshot" ON metrics_snapshot
    FOR ALL USING (true) WITH CHECK (true);


-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION QUERIES (run after creation to confirm)
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- SELECT table_name, column_name, data_type, is_nullable
-- FROM information_schema.columns
-- WHERE table_schema = 'public'
--   AND table_name IN ('nova_memory', 'plan_events', 'enrichment_log', 'metrics_snapshot')
-- ORDER BY table_name, ordinal_position;
--
-- ═══════════════════════════════════════════════════════════════════════════════
-- DONE. 4 tables with indexes, RLS, and policies for disaster recovery.
-- ═══════════════════════════════════════════════════════════════════════════════
