-- ============================================================================
-- Create plan_events table for durable plan storage
-- Run this in Supabase SQL Editor: https://supabase.com/dashboard/project/trpynqjatlhatxpzrvgt/sql
-- ============================================================================

-- 1. Create the table
CREATE TABLE IF NOT EXISTS plan_events (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        TEXT,
    plan_id         TEXT,
    event_type      TEXT            NOT NULL,
    client_name     TEXT,
    industry        TEXT,
    budget          TEXT,
    roles           JSONB,
    locations       JSONB,
    channels_selected JSONB,
    user_email      TEXT,
    user_name       TEXT,
    generation_time_ms FLOAT,
    file_size_bytes INTEGER,
    error_message   TEXT,
    enrichment_apis JSONB,
    user_id         TEXT            DEFAULT 'system',
    payload         JSONB,
    version         INTEGER,
    metadata        JSONB,
    timestamp       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ     DEFAULT NOW()
);

-- 2. Add comment
COMMENT ON TABLE plan_events IS 'Durable event log for media plan generation (event-sourced + request logging)';

-- 3. Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_plan_events_created_at ON plan_events (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_plan_events_client_name ON plan_events (client_name);
CREATE INDEX IF NOT EXISTS idx_plan_events_user_email ON plan_events (user_email);
CREATE INDEX IF NOT EXISTS idx_plan_events_plan_id ON plan_events (plan_id);
CREATE INDEX IF NOT EXISTS idx_plan_events_event_type ON plan_events (event_type);

-- 4. Enable Row Level Security
ALTER TABLE plan_events ENABLE ROW LEVEL SECURITY;

-- 5. Service role full access policy (server-side only)
CREATE POLICY "Service role full access on plan_events"
    ON plan_events
    FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- 6. Verify
SELECT
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'plan_events'
  AND table_schema = 'public'
ORDER BY ordinal_position;
