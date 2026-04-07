-- S47: Persistent plan storage tables
-- Run against Supabase SQL Editor or via psql

-- Table 1: Saved Plans (user-initiated saves via "Save Plan" button)
CREATE TABLE IF NOT EXISTS nova_saved_plans (
    id BIGSERIAL PRIMARY KEY,
    user_email TEXT NOT NULL DEFAULT 'unknown',
    plan_name TEXT NOT NULL DEFAULT 'Untitled Plan',
    plan_data JSONB DEFAULT '{}',
    industry TEXT DEFAULT '',
    location TEXT DEFAULT '',
    budget NUMERIC DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for user lookups
CREATE INDEX IF NOT EXISTS idx_saved_plans_email ON nova_saved_plans(user_email);
CREATE INDEX IF NOT EXISTS idx_saved_plans_created ON nova_saved_plans(created_at DESC);

-- Table 2: Generated Plans (auto-persisted for Slack download links)
CREATE TABLE IF NOT EXISTS nova_generated_plans (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT UNIQUE NOT NULL,
    zip_data TEXT,  -- base64 encoded ZIP
    filename TEXT DEFAULT 'Media_Plan.zip',
    client_name TEXT DEFAULT '',
    industry TEXT DEFAULT '',
    user_email TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for job_id lookups (used by /api/jobs/<id>)
CREATE INDEX IF NOT EXISTS idx_generated_plans_job_id ON nova_generated_plans(job_id);

-- Auto-cleanup: delete generated plans older than 30 days
-- (run manually or via pg_cron if available)
-- DELETE FROM nova_generated_plans WHERE created_at < NOW() - INTERVAL '30 days';

-- RLS policies
ALTER TABLE nova_saved_plans ENABLE ROW LEVEL SECURITY;
ALTER TABLE nova_generated_plans ENABLE ROW LEVEL SECURITY;

-- Allow service role full access (server-side)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies WHERE tablename = 'nova_saved_plans' AND policyname = 'Service role full access saved_plans'
    ) THEN
        CREATE POLICY "Service role full access saved_plans" ON nova_saved_plans FOR ALL USING (true);
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies WHERE tablename = 'nova_generated_plans' AND policyname = 'Service role full access generated_plans'
    ) THEN
        CREATE POLICY "Service role full access generated_plans" ON nova_generated_plans FOR ALL USING (true);
    END IF;
END
$$;
