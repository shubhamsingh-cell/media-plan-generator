-- ============================================================================
-- Create social_campaign_metrics + social_metrics_sync_log tables
-- Run in Supabase SQL Editor: https://supabase.com/dashboard/project/trpynqjatlhatxpzrvgt/sql
--
-- Purpose: Replace Supermetrics -> Sheets pipeline with direct Meta + Google
-- Ads -> Supabase sync. Background job in app.py pulls daily campaign rows
-- every 6h; Nova chatbot exposes query_meta_performance and
-- query_google_ads_performance tools that read from social_campaign_metrics.
-- ============================================================================

-- 1. Daily per-campaign metrics (unified schema across Meta + Google Ads)
CREATE TABLE IF NOT EXISTS social_campaign_metrics (
    id              BIGSERIAL       PRIMARY KEY,
    platform        TEXT            NOT NULL CHECK (platform IN ('meta', 'google_ads')),
    account_id      TEXT            NOT NULL,
    campaign_id     TEXT            NOT NULL,
    campaign_name   TEXT,
    objective       TEXT,
    date            DATE            NOT NULL,
    -- Core metrics (unified)
    spend           NUMERIC(14, 4)  NOT NULL DEFAULT 0,
    impressions     BIGINT          NOT NULL DEFAULT 0,
    clicks          BIGINT          NOT NULL DEFAULT 0,
    conversions     NUMERIC(14, 4)  NOT NULL DEFAULT 0,
    -- Derived metrics (computed by sync job; null if undefined)
    ctr             NUMERIC(8, 4),
    cpc             NUMERIC(10, 4),
    cpa             NUMERIC(10, 4),
    cpm             NUMERIC(10, 4),
    -- Currency + raw payload for auditability
    currency        TEXT            DEFAULT 'USD',
    raw             JSONB,
    synced_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    -- Idempotent upsert key
    CONSTRAINT social_campaign_metrics_unique UNIQUE (platform, account_id, campaign_id, date)
);

COMMENT ON TABLE social_campaign_metrics IS 'Daily per-campaign performance metrics from Meta Ads and Google Ads (live sync, replaces Supermetrics->Sheets)';

-- 2. Indexes for common Nova query patterns
CREATE INDEX IF NOT EXISTS idx_social_metrics_platform_date
    ON social_campaign_metrics (platform, date DESC);
CREATE INDEX IF NOT EXISTS idx_social_metrics_campaign_date
    ON social_campaign_metrics (platform, campaign_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_social_metrics_synced_at
    ON social_campaign_metrics (synced_at DESC);

-- 3. Sync run log (for monitoring + debugging)
CREATE TABLE IF NOT EXISTS social_metrics_sync_log (
    id              BIGSERIAL       PRIMARY KEY,
    platform        TEXT            NOT NULL,
    started_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          TEXT            NOT NULL,  -- 'ok' | 'error' | 'skipped' | 'partial'
    rows_upserted   INTEGER         NOT NULL DEFAULT 0,
    duration_ms     INTEGER,
    error           TEXT,
    metadata        JSONB
);

COMMENT ON TABLE social_metrics_sync_log IS 'Audit trail for social_metrics background sync runs';

CREATE INDEX IF NOT EXISTS idx_sync_log_platform_started
    ON social_metrics_sync_log (platform, started_at DESC);

-- 4. Row Level Security (service role only; nothing user-facing)
ALTER TABLE social_campaign_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE social_metrics_sync_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role full access on social_campaign_metrics"
    ON social_campaign_metrics
    FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "Service role full access on social_metrics_sync_log"
    ON social_metrics_sync_log
    FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- 5. Verify
SELECT
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_name IN ('social_campaign_metrics', 'social_metrics_sync_log')
  AND table_schema = 'public'
ORDER BY table_name, ordinal_position;
