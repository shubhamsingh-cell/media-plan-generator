-- Migration 003: Create nova_memory table for Nova AI persistent memory
-- Stores conversation summaries, learned facts, and user preferences across sessions.
-- Used by nova_memory.py for cross-session continuity.

CREATE TABLE IF NOT EXISTS nova_memory (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT NOT NULL,
    memory_type TEXT NOT NULL CHECK (memory_type IN ('short_term', 'long_term', 'preference')),
    key TEXT NOT NULL DEFAULT '',
    value JSONB NOT NULL DEFAULT '{}'::jsonb,
    content TEXT NOT NULL DEFAULT '',
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    expires_at TIMESTAMPTZ
);

-- Index for user lookups
CREATE INDEX IF NOT EXISTS idx_nova_memory_user ON nova_memory(user_id);

-- Composite index for user + type filtering
CREATE INDEX IF NOT EXISTS idx_nova_memory_type ON nova_memory(user_id, memory_type);

-- Index for TTL-based cleanup of expired entries
CREATE INDEX IF NOT EXISTS idx_nova_memory_expires ON nova_memory(expires_at)
    WHERE expires_at IS NOT NULL;

-- Enable Row Level Security
ALTER TABLE nova_memory ENABLE ROW LEVEL SECURITY;

-- Policy: allow all operations with service role key (server-side only)
CREATE POLICY "Allow all operations" ON nova_memory
    FOR ALL USING (true) WITH CHECK (true);

-- Updated_at trigger
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
