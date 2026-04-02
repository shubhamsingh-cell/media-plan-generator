-- =============================================================================
-- Nova Platform Consolidated Schema - Supabase Tables
-- Date: 2026-03-24
-- Description: New tables for the 3-module consolidated platform
--              (Command Center, Intelligence Hub, Nova AI)
--
-- Tables:
--   1. [REMOVED] nova_campaigns    -- Dropped in S34 (dead since S19)
--   2. [REMOVED] nova_module_usage -- Dropped in S34 (dead since S19)
--   3. nova_module_health    -- Health snapshot time-series
--   4. nova_data_cache       -- Structured data cache with TTL
--   5. nova_user_preferences -- User settings and personalization
--
-- Follows patterns from nova_schema_additions.sql:
--   - UUID primary keys with gen_random_uuid()
--   - TIMESTAMPTZ with DEFAULT NOW()
--   - JSONB for flexible metadata
--   - RLS enabled on all tables
--   - service_role full access policies
--   - Targeted indexes for query patterns
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. [REMOVED] nova_campaigns -- Dropped in S34 (dead since S19, never populated)
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- 2. [REMOVED] nova_module_usage -- Dropped in S34 (dead since S19, replaced by PostHog)
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- 3. Nova Module Health -- Health snapshot time-series
--    Periodic snapshots of each module's health for dashboards and alerting.
--    Written by a background health-check task (e.g., every 5 minutes).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nova_module_health (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    module_name     VARCHAR(64)     NOT NULL,       -- command_center | intelligence_hub | nova_ai
    health_score    NUMERIC(4, 2)   NOT NULL,       -- 0.00 to 1.00
    error_rate      NUMERIC(6, 4),                  -- Errors / total requests (0.0000 to 1.0000)
    p50_latency_ms  INTEGER,                        -- Median latency
    p95_latency_ms  INTEGER,                        -- 95th percentile latency
    p99_latency_ms  INTEGER,                        -- 99th percentile latency
    active_providers INTEGER,                       -- Number of healthy LLM providers
    data_sources_up INTEGER,                        -- Number of healthy data sources
    request_count   INTEGER,                        -- Requests in the measurement window
    metadata        JSONB           DEFAULT '{}'::jsonb,  -- Provider-level breakdown, etc.
    checked_at      TIMESTAMPTZ     DEFAULT NOW()
);

-- Query patterns: latest health per module, time-series for graphing
CREATE INDEX IF NOT EXISTS idx_nova_module_health_module ON nova_module_health(module_name);
CREATE INDEX IF NOT EXISTS idx_nova_module_health_checked_at ON nova_module_health(checked_at DESC);

-- Composite: latest health check per module (common dashboard query)
CREATE INDEX IF NOT EXISTS idx_nova_module_health_module_time
    ON nova_module_health(module_name, checked_at DESC);

-- ---------------------------------------------------------------------------
-- 4. Nova Data Cache -- Structured data cache with TTL
--    Persistent L3 cache for expensive API call results (economic data,
--    scraped content, analysis results). Complements in-memory L1 and
--    Upstash Redis L2 caches.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nova_data_cache (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    cache_key       VARCHAR(512)    NOT NULL UNIQUE,  -- Deterministic key (source|query hash)
    data            JSONB           NOT NULL,         -- Cached response data
    source          VARCHAR(64)     NOT NULL,         -- Data source identifier
    data_type       VARCHAR(64),                      -- Category: jobs, economic, skills, etc.
    ttl_seconds     INTEGER         NOT NULL DEFAULT 3600,  -- Time-to-live in seconds
    hit_count       INTEGER         DEFAULT 0,        -- Number of times this cache entry was read
    created_at      TIMESTAMPTZ     DEFAULT NOW(),
    expires_at      TIMESTAMPTZ     NOT NULL,         -- Pre-computed: created_at + ttl_seconds
    metadata        JSONB           DEFAULT '{}'::jsonb  -- Query params, freshness info, etc.
);

-- Query patterns: key lookup, expiry cleanup, source analysis
CREATE UNIQUE INDEX IF NOT EXISTS idx_nova_data_cache_key ON nova_data_cache(cache_key);
CREATE INDEX IF NOT EXISTS idx_nova_data_cache_source ON nova_data_cache(source);
CREATE INDEX IF NOT EXISTS idx_nova_data_cache_expires_at ON nova_data_cache(expires_at);
CREATE INDEX IF NOT EXISTS idx_nova_data_cache_data_type ON nova_data_cache(data_type);

-- ---------------------------------------------------------------------------
-- 5. Nova User Preferences -- User settings and personalization
--    Stores per-user settings for the platform UI and behavior.
--    Uses anonymous user_id from localStorage (same as nova_conversations).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nova_user_preferences (
    id                  UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             VARCHAR(255)    NOT NULL UNIQUE,  -- Anonymous user ID
    favorites           JSONB           DEFAULT '[]'::jsonb,    -- Saved campaigns, queries, etc.
    recent_actions      JSONB           DEFAULT '[]'::jsonb,    -- Last N actions for quick access
    sidebar_collapsed   BOOLEAN         DEFAULT FALSE,
    theme               VARCHAR(16)     DEFAULT 'dark',         -- dark | light
    last_module         VARCHAR(64)     DEFAULT 'command_center', -- Last active module
    onboarding_complete BOOLEAN         DEFAULT FALSE,
    notification_prefs  JSONB           DEFAULT '{}'::jsonb,    -- Email, in-app notification settings
    metadata            JSONB           DEFAULT '{}'::jsonb,    -- Feature flags, A/B test groups, etc.
    created_at          TIMESTAMPTZ     DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     DEFAULT NOW()
);

-- Query patterns: user lookup (primary), onboarding status
CREATE UNIQUE INDEX IF NOT EXISTS idx_nova_user_preferences_user_id ON nova_user_preferences(user_id);
CREATE INDEX IF NOT EXISTS idx_nova_user_preferences_onboarding
    ON nova_user_preferences(onboarding_complete) WHERE onboarding_complete = FALSE;

-- Auto-update updated_at on modification
CREATE TRIGGER trg_nova_user_preferences_updated_at
    BEFORE UPDATE ON nova_user_preferences
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =============================================================================
-- 6. Enable Row Level Security on all new tables
-- =============================================================================
ALTER TABLE nova_module_health ENABLE ROW LEVEL SECURITY;
ALTER TABLE nova_data_cache ENABLE ROW LEVEL SECURITY;
ALTER TABLE nova_user_preferences ENABLE ROW LEVEL SECURITY;

-- =============================================================================
-- 7. Service Role Policies (full access for backend)
-- =============================================================================
CREATE POLICY service_role_nova_module_health ON nova_module_health
    FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY service_role_nova_data_cache ON nova_data_cache
    FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY service_role_nova_user_preferences ON nova_user_preferences
    FOR ALL TO service_role USING (true) WITH CHECK (true);

-- =============================================================================
-- 8. Helper Functions
-- =============================================================================

-- Cleanup expired cache entries (run periodically via cron or background task)
CREATE OR REPLACE FUNCTION nova_cleanup_expired_cache()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM nova_data_cache
    WHERE expires_at < NOW();
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Upsert into nova_data_cache with TTL refresh
CREATE OR REPLACE FUNCTION nova_cache_upsert(
    p_cache_key VARCHAR,
    p_data JSONB,
    p_source VARCHAR,
    p_data_type VARCHAR,
    p_ttl_seconds INTEGER DEFAULT 3600
)
RETURNS UUID AS $$
DECLARE
    result_id UUID;
BEGIN
    INSERT INTO nova_data_cache (cache_key, data, source, data_type, ttl_seconds, expires_at)
    VALUES (
        p_cache_key,
        p_data,
        p_source,
        p_data_type,
        p_ttl_seconds,
        NOW() + (p_ttl_seconds || ' seconds')::INTERVAL
    )
    ON CONFLICT (cache_key) DO UPDATE SET
        data = EXCLUDED.data,
        source = EXCLUDED.source,
        data_type = EXCLUDED.data_type,
        ttl_seconds = EXCLUDED.ttl_seconds,
        expires_at = NOW() + (EXCLUDED.ttl_seconds || ' seconds')::INTERVAL,
        hit_count = nova_data_cache.hit_count  -- preserve hit count on refresh
    RETURNING id INTO result_id;
    RETURN result_id;
END;
$$ LANGUAGE plpgsql;

-- Get or create user preferences (returns the full row)
CREATE OR REPLACE FUNCTION nova_get_or_create_preferences(p_user_id VARCHAR)
RETURNS nova_user_preferences AS $$
DECLARE
    result nova_user_preferences;
BEGIN
    SELECT * INTO result FROM nova_user_preferences WHERE user_id = p_user_id;
    IF result IS NULL THEN
        INSERT INTO nova_user_preferences (user_id)
        VALUES (p_user_id)
        RETURNING * INTO result;
    END IF;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- [REMOVED] nova_module_usage_stats function -- dropped with nova_module_usage table in S34
