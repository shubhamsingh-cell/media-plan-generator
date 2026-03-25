-- =============================================================================
-- Nova Phase 3+4 Enterprise Features - Supabase Schema Additions
-- Date: 2026-03-24
-- Description: Tables for conversations, documents, avatars, and share links
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Nova Conversations Table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nova_conversations (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         VARCHAR(255)    NOT NULL,  -- Anonymous user ID (from localStorage)
    title           VARCHAR(255)    DEFAULT 'New Chat',
    messages        JSONB           DEFAULT '[]'::jsonb,
    theme           VARCHAR(16)     DEFAULT 'dark',  -- 'dark' or 'light'
    avatar_style    VARCHAR(64)     DEFAULT 'default',
    created_at      TIMESTAMPTZ     DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     DEFAULT NOW(),
    shared_link_id  UUID            UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_nova_conversations_user_id ON nova_conversations(user_id);
CREATE INDEX IF NOT EXISTS idx_nova_conversations_shared_link_id ON nova_conversations(shared_link_id);
CREATE INDEX IF NOT EXISTS idx_nova_conversations_created_at ON nova_conversations(created_at DESC);

-- ---------------------------------------------------------------------------
-- 2. Nova Documents Table (for RAG)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nova_documents (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID            NOT NULL REFERENCES nova_conversations(id) ON DELETE CASCADE,
    filename        VARCHAR(255)    NOT NULL,
    file_path       VARCHAR(512)    NOT NULL,  -- Local path or S3 key
    content_text    TEXT,                      -- Extracted plain text for RAG embedding
    file_type       VARCHAR(16)     NOT NULL,  -- 'pdf', 'docx', 'txt', 'xlsx'
    size_bytes      INTEGER,
    uploaded_at     TIMESTAMPTZ     DEFAULT NOW(),
    metadata        JSONB           DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_nova_documents_conversation_id ON nova_documents(conversation_id);
CREATE INDEX IF NOT EXISTS idx_nova_documents_file_type ON nova_documents(file_type);
CREATE INDEX IF NOT EXISTS idx_nova_documents_uploaded_at ON nova_documents(uploaded_at DESC);

-- ---------------------------------------------------------------------------
-- 3. Nova Shared Conversations Table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nova_shared_conversations (
    share_id        UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID            NOT NULL UNIQUE REFERENCES nova_conversations(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ     DEFAULT NOW(),
    expires_at      TIMESTAMPTZ,                -- Optional TTL, NULL = never expires
    access_count    INTEGER         DEFAULT 0,
    metadata        JSONB           DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_nova_shared_conversations_conversation_id ON nova_shared_conversations(conversation_id);
CREATE INDEX IF NOT EXISTS idx_nova_shared_conversations_expires_at ON nova_shared_conversations(expires_at);

-- ---------------------------------------------------------------------------
-- 4. Nova Avatars Table  [DEPRECATED 2026-03-25]
--    This table is unused. Avatar display is handled via CSS in the chat UI.
--    Kept for reference only -- do NOT add new code that reads/writes this table.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nova_avatars (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    persona_name    VARCHAR(128)    NOT NULL UNIQUE,
    image_url       VARCHAR(512),
    style           VARCHAR(32)     NOT NULL,  -- 'ai-generated', 'gradient', 'emoji', 'initials'
    color           VARCHAR(7),                -- Hex color for gradient/initials
    metadata        JSONB           DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ     DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nova_avatars_persona_name ON nova_avatars(persona_name);
CREATE INDEX IF NOT EXISTS idx_nova_avatars_style ON nova_avatars(style);

-- ---------------------------------------------------------------------------
-- 5. Update Trigger for nova_conversations
-- ---------------------------------------------------------------------------
CREATE TRIGGER trg_nova_conversations_updated_at
    BEFORE UPDATE ON nova_conversations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ---------------------------------------------------------------------------
-- 6. Enable RLS
-- ---------------------------------------------------------------------------
ALTER TABLE nova_conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE nova_documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE nova_shared_conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE nova_avatars ENABLE ROW LEVEL SECURITY;

-- Permissive policies: allow full access for service_role
CREATE POLICY service_role_nova_conversations ON nova_conversations FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_nova_documents ON nova_documents FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_nova_shared_conversations ON nova_shared_conversations FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_nova_avatars ON nova_avatars FOR ALL TO service_role USING (true) WITH CHECK (true);

-- ---------------------------------------------------------------------------
-- 7. Helper Functions
-- ---------------------------------------------------------------------------

-- Get or create anonymous user session
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

-- Cleanup expired shared links
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
