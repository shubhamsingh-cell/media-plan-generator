-- =============================================================================
-- Migration 001: nova_conversations schema upgrade
-- From: row-per-turn (integer id, user_message, assistant_response)
-- To:   document-model (UUID id, messages JSONB array, user_id, title, etc.)
--
-- SAFE: Preserves all 7 existing rows by migrating them into the new format
-- RUN:  Paste into Supabase SQL Editor > Run
-- DATE: 2026-04-03
-- =============================================================================

-- Step 1: Rename old table to preserve data
ALTER TABLE nova_conversations RENAME TO nova_conversations_legacy;

-- Step 2: Drop old indexes (they reference the old table name)
DROP INDEX IF EXISTS idx_nova_conversations_user_id;
DROP INDEX IF EXISTS idx_nova_conversations_shared_link_id;
DROP INDEX IF EXISTS idx_nova_conversations_created_at;

-- Step 3: Drop old trigger if exists
DROP TRIGGER IF EXISTS trg_nova_conversations_updated_at ON nova_conversations_legacy;

-- Step 4: Drop old RLS policies
DROP POLICY IF EXISTS service_role_nova_conversations ON nova_conversations_legacy;

-- Step 5: Create the new table with correct document-model schema
CREATE TABLE nova_conversations (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         VARCHAR(255)    NOT NULL DEFAULT 'anonymous',
    title           VARCHAR(255)    DEFAULT 'New Chat',
    messages        JSONB           DEFAULT '[]'::jsonb,
    theme           VARCHAR(16)     DEFAULT 'dark',
    avatar_style    VARCHAR(64)     DEFAULT 'default',
    shared_link_id  TEXT            UNIQUE,
    session_token   TEXT,
    created_at      TIMESTAMPTZ     DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     DEFAULT NOW()
);

-- Step 6: Create indexes
CREATE INDEX idx_nova_conversations_user_id ON nova_conversations(user_id);
CREATE INDEX idx_nova_conversations_shared_link_id ON nova_conversations(shared_link_id);
CREATE INDEX idx_nova_conversations_created_at ON nova_conversations(created_at DESC);
CREATE INDEX idx_nova_conversations_updated_at ON nova_conversations(updated_at DESC);

-- Step 7: Create updated_at trigger
-- First ensure the trigger function exists
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_nova_conversations_updated_at
    BEFORE UPDATE ON nova_conversations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Step 8: Enable RLS
ALTER TABLE nova_conversations ENABLE ROW LEVEL SECURITY;

-- Step 9: Create service_role policy
CREATE POLICY service_role_nova_conversations ON nova_conversations
    FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Step 10: Migrate legacy data into one document-model row
-- All 7 rows belong to conversation_id = 'conv-1774336934009-rwam4a'
-- We consolidate them into a single row with messages JSONB array
INSERT INTO nova_conversations (id, user_id, title, messages, created_at, updated_at)
SELECT
    gen_random_uuid(),
    'anonymous',
    COALESCE(
        (SELECT l2.user_message FROM nova_conversations_legacy l2 ORDER BY l2.timestamp ASC LIMIT 1),
        'New Chat'
    ),
    (
        SELECT jsonb_agg(msg ORDER BY msg_order)
        FROM (
            SELECT
                row_number() OVER (ORDER BY l.timestamp ASC, l.id ASC) * 2 - 1 AS msg_order,
                jsonb_build_object(
                    'role', 'user',
                    'content', l.user_message,
                    'timestamp', l.timestamp::text
                ) AS msg
            FROM nova_conversations_legacy l
            WHERE l.user_message IS NOT NULL AND l.user_message != ''
            UNION ALL
            SELECT
                row_number() OVER (ORDER BY l.timestamp ASC, l.id ASC) * 2 AS msg_order,
                jsonb_build_object(
                    'role', 'assistant',
                    'content', l.assistant_response,
                    'timestamp', l.timestamp::text,
                    'model_used', COALESCE(NULLIF(l.model_used, ''), 'unknown'),
                    'sources', CASE
                        WHEN l.sources IS NOT NULL AND l.sources != '[]' AND l.sources != ''
                        THEN l.sources::jsonb
                        ELSE '[]'::jsonb
                    END,
                    'confidence', l.confidence
                ) AS msg
            FROM nova_conversations_legacy l
            WHERE l.assistant_response IS NOT NULL AND l.assistant_response != ''
        ) sub
    ),
    (SELECT MIN(timestamp) FROM nova_conversations_legacy),
    (SELECT MAX(timestamp) FROM nova_conversations_legacy);

-- Step 11: Create dependent tables that reference the new schema

-- Documents table (for RAG)
CREATE TABLE IF NOT EXISTS nova_documents (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID            NOT NULL REFERENCES nova_conversations(id) ON DELETE CASCADE,
    filename        VARCHAR(255)    NOT NULL,
    file_path       VARCHAR(512)    NOT NULL,
    content_text    TEXT,
    file_type       VARCHAR(16)     NOT NULL,
    size_bytes      INTEGER,
    uploaded_at     TIMESTAMPTZ     DEFAULT NOW(),
    metadata        JSONB           DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_nova_documents_conversation_id ON nova_documents(conversation_id);
CREATE INDEX IF NOT EXISTS idx_nova_documents_file_type ON nova_documents(file_type);
CREATE INDEX IF NOT EXISTS idx_nova_documents_uploaded_at ON nova_documents(uploaded_at DESC);

ALTER TABLE nova_documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY service_role_nova_documents ON nova_documents
    FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Shared conversations table
CREATE TABLE IF NOT EXISTS nova_shared_conversations (
    share_id        UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID            NOT NULL UNIQUE REFERENCES nova_conversations(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ     DEFAULT NOW(),
    expires_at      TIMESTAMPTZ,
    access_count    INTEGER         DEFAULT 0,
    metadata        JSONB           DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_nova_shared_conversations_conversation_id ON nova_shared_conversations(conversation_id);
CREATE INDEX IF NOT EXISTS idx_nova_shared_conversations_expires_at ON nova_shared_conversations(expires_at);

ALTER TABLE nova_shared_conversations ENABLE ROW LEVEL SECURITY;
CREATE POLICY service_role_nova_shared_conversations ON nova_shared_conversations
    FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Step 12: Recreate helper functions
CREATE OR REPLACE FUNCTION nova_get_or_create_user(user_id_input VARCHAR)
RETURNS VARCHAR AS $$
DECLARE
    result VARCHAR;
BEGIN
    SELECT user_id INTO result FROM nova_conversations WHERE user_id = user_id_input LIMIT 1;
    IF result IS NULL THEN
        result := user_id_input;
    END IF;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION nova_cleanup_expired_shares()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM nova_shared_conversations
    WHERE expires_at IS NOT NULL AND expires_at < NOW();
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- VERIFICATION: Run after migration to confirm
-- =============================================================================
-- SELECT id, user_id, title, jsonb_array_length(messages) as msg_count, created_at, updated_at
-- FROM nova_conversations;
--
-- To drop legacy table after verifying (optional, can keep for safety):
-- DROP TABLE nova_conversations_legacy;
-- =============================================================================
