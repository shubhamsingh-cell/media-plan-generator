-- S37: Create missing tables for plan tracking, document storage, and login audit
-- Run this in Supabase SQL Editor

-- 1. plan_events -- durable storage for every generated media plan
CREATE TABLE IF NOT EXISTS plan_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL DEFAULT 'plan_generated',
    plan_id TEXT,
    client_name TEXT,
    industry TEXT,
    budget TEXT,
    roles JSONB DEFAULT '[]',
    locations JSONB DEFAULT '[]',
    channels_selected JSONB DEFAULT '{}',
    channel_count INTEGER DEFAULT 0,
    user_email TEXT,
    user_name TEXT,
    generation_time_ms FLOAT,
    file_size_bytes INTEGER,
    error_message TEXT,
    enrichment_apis JSONB DEFAULT '[]',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_plan_events_created ON plan_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_plan_events_client ON plan_events(client_name);
CREATE INDEX IF NOT EXISTS idx_plan_events_user ON plan_events(user_email);

ALTER TABLE plan_events ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Service role full access" ON plan_events FOR ALL USING (true);

-- 2. nova_documents -- document storage
CREATE TABLE IF NOT EXISTS nova_documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT,
    title TEXT,
    doc_type TEXT DEFAULT 'plan',
    content JSONB DEFAULT '{}',
    file_url TEXT,
    file_size_bytes INTEGER,
    tags JSONB DEFAULT '[]',
    shared_link_id TEXT UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nova_documents_user ON nova_documents(user_id);
CREATE INDEX IF NOT EXISTS idx_nova_documents_type ON nova_documents(doc_type);

ALTER TABLE nova_documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Service role full access" ON nova_documents FOR ALL USING (true);

-- 3. nova_login_log -- track every OAuth sign-in
CREATE TABLE IF NOT EXISTS nova_login_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_email TEXT NOT NULL,
    user_name TEXT,
    provider TEXT DEFAULT 'google',
    ip_hash TEXT,
    user_agent TEXT,
    product TEXT DEFAULT 'nova',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nova_login_log_email ON nova_login_log(user_email);
CREATE INDEX IF NOT EXISTS idx_nova_login_log_created ON nova_login_log(created_at DESC);

ALTER TABLE nova_login_log ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Service role full access" ON nova_login_log FOR ALL USING (true);

-- 4. Migrate nova_conversations -- add document-model columns to existing table
DO $$
BEGIN
    -- Add columns if they don't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'nova_conversations' AND column_name = 'user_id') THEN
        ALTER TABLE nova_conversations ADD COLUMN user_id TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'nova_conversations' AND column_name = 'title') THEN
        ALTER TABLE nova_conversations ADD COLUMN title TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'nova_conversations' AND column_name = 'messages') THEN
        ALTER TABLE nova_conversations ADD COLUMN messages JSONB DEFAULT '[]';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'nova_conversations' AND column_name = 'theme') THEN
        ALTER TABLE nova_conversations ADD COLUMN theme TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'nova_conversations' AND column_name = 'avatar_style') THEN
        ALTER TABLE nova_conversations ADD COLUMN avatar_style TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'nova_conversations' AND column_name = 'shared_link_id') THEN
        ALTER TABLE nova_conversations ADD COLUMN shared_link_id TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'nova_conversations' AND column_name = 'session_token') THEN
        ALTER TABLE nova_conversations ADD COLUMN session_token TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'nova_conversations' AND column_name = 'updated_at') THEN
        ALTER TABLE nova_conversations ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'nova_conversations' AND column_name = 'uuid_id') THEN
        ALTER TABLE nova_conversations ADD COLUMN uuid_id UUID DEFAULT gen_random_uuid();
    END IF;
END $$;
