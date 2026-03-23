-- =============================================================================
-- Media Plan Generator - Supabase Schema
-- Project: media-plan-generator
-- Date: 2026-03-23
-- Description: Complete database schema for persistent cache, knowledge base,
--              channel benchmarks, salary data, compliance rules, market trends,
--              vendor profiles, and supply repository.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. FUNCTIONS
-- ---------------------------------------------------------------------------

-- Auto-update updated_at column on row modification
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Cleanup expired cache entries; returns number of rows deleted
CREATE OR REPLACE FUNCTION cleanup_expired_cache()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM cache WHERE expires_at < NOW();
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------------------
-- 2. TABLES
-- ---------------------------------------------------------------------------

-- L3 persistent cache
CREATE TABLE IF NOT EXISTS cache (
    key         TEXT        PRIMARY KEY,
    data        JSONB       NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL,
    category    TEXT        DEFAULT 'general',
    hit_count   INTEGER     DEFAULT 0
);

-- Knowledge entries by category/key
CREATE TABLE IF NOT EXISTS knowledge_base (
    id          BIGSERIAL   PRIMARY KEY,
    category    TEXT        NOT NULL,
    key         TEXT        NOT NULL,
    data        JSONB       NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (category, key)
);

-- Job board pricing/performance benchmarks
CREATE TABLE IF NOT EXISTS channel_benchmarks (
    id              BIGSERIAL       PRIMARY KEY,
    channel         TEXT            NOT NULL,
    industry        TEXT            DEFAULT 'overall',
    cpc             NUMERIC(10, 4),
    cpa             NUMERIC(10, 4),
    pricing_model   TEXT,
    metadata        JSONB,
    created_at      TIMESTAMPTZ     DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     DEFAULT NOW(),
    UNIQUE (channel, industry)
);

-- BLS salary benchmarks
CREATE TABLE IF NOT EXISTS salary_data (
    id          BIGSERIAL       PRIMARY KEY,
    role        TEXT            NOT NULL,
    location    TEXT,
    median      NUMERIC(10, 2),
    p10         NUMERIC(10, 2),
    p90         NUMERIC(10, 2),
    metadata    JSONB,
    created_at  TIMESTAMPTZ     DEFAULT NOW(),
    updated_at  TIMESTAMPTZ     DEFAULT NOW(),
    UNIQUE (role, location)
);

-- Regulatory rules by jurisdiction
CREATE TABLE IF NOT EXISTS compliance_rules (
    id              BIGSERIAL   PRIMARY KEY,
    rule_type       TEXT        NOT NULL,
    jurisdiction    TEXT        NOT NULL,
    description     TEXT,
    status          TEXT        DEFAULT 'active',
    effective_date  DATE,
    metadata        JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (rule_type, jurisdiction)
);

-- Market/pricing trends over time
CREATE TABLE IF NOT EXISTS market_trends (
    id          BIGSERIAL   PRIMARY KEY,
    category    TEXT        NOT NULL,
    title       TEXT,
    source      TEXT,
    url         TEXT,
    metadata    JSONB,
    scraped_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE NULLS NOT DISTINCT (category, title, source)
);

-- Job board/vendor profiles
CREATE TABLE IF NOT EXISTS vendor_profiles (
    id              BIGSERIAL   PRIMARY KEY,
    name            TEXT        NOT NULL UNIQUE,
    category        TEXT,
    description     TEXT,
    website_url     TEXT,
    pricing_info    JSONB,
    features        JSONB,
    ratings         JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Publishers/distribution networks
CREATE TABLE IF NOT EXISTS supply_repository (
    id          BIGSERIAL   PRIMARY KEY,
    name        TEXT        NOT NULL UNIQUE,
    category    TEXT,
    countries   TEXT[]      DEFAULT '{}',
    description TEXT,
    performance JSONB,
    metadata    JSONB,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- 3. INDEXES
-- ---------------------------------------------------------------------------

-- cache
CREATE INDEX IF NOT EXISTS idx_cache_expires_at ON cache (expires_at);
CREATE INDEX IF NOT EXISTS idx_cache_category   ON cache (category);

-- knowledge_base
CREATE INDEX IF NOT EXISTS idx_knowledge_base_category     ON knowledge_base (category);
CREATE INDEX IF NOT EXISTS idx_knowledge_base_category_key ON knowledge_base (category, key);

-- channel_benchmarks
CREATE INDEX IF NOT EXISTS idx_channel_benchmarks_channel  ON channel_benchmarks (channel);
CREATE INDEX IF NOT EXISTS idx_channel_benchmarks_industry ON channel_benchmarks (industry);

-- salary_data
CREATE INDEX IF NOT EXISTS idx_salary_data_role     ON salary_data (role);
CREATE INDEX IF NOT EXISTS idx_salary_data_location ON salary_data (location);

-- compliance_rules
CREATE INDEX IF NOT EXISTS idx_compliance_rules_rule_type     ON compliance_rules (rule_type);
CREATE INDEX IF NOT EXISTS idx_compliance_rules_jurisdiction  ON compliance_rules (jurisdiction);
CREATE INDEX IF NOT EXISTS idx_compliance_rules_status        ON compliance_rules (status);

-- market_trends
CREATE INDEX IF NOT EXISTS idx_market_trends_category   ON market_trends (category);
CREATE INDEX IF NOT EXISTS idx_market_trends_scraped_at ON market_trends (scraped_at DESC);

-- vendor_profiles
CREATE INDEX IF NOT EXISTS idx_vendor_profiles_category ON vendor_profiles (category);

-- supply_repository
CREATE INDEX IF NOT EXISTS idx_supply_repository_category  ON supply_repository (category);
CREATE INDEX IF NOT EXISTS idx_supply_repository_countries ON supply_repository USING GIN (countries);

-- ---------------------------------------------------------------------------
-- 4. ROW LEVEL SECURITY
-- ---------------------------------------------------------------------------

ALTER TABLE cache              ENABLE ROW LEVEL SECURITY;
ALTER TABLE knowledge_base     ENABLE ROW LEVEL SECURITY;
ALTER TABLE channel_benchmarks ENABLE ROW LEVEL SECURITY;
ALTER TABLE salary_data        ENABLE ROW LEVEL SECURITY;
ALTER TABLE compliance_rules   ENABLE ROW LEVEL SECURITY;
ALTER TABLE market_trends      ENABLE ROW LEVEL SECURITY;
ALTER TABLE vendor_profiles    ENABLE ROW LEVEL SECURITY;
ALTER TABLE supply_repository  ENABLE ROW LEVEL SECURITY;

-- Permissive policies: allow full access for service_role
CREATE POLICY service_role_cache              ON cache              FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_knowledge_base     ON knowledge_base     FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_channel_benchmarks ON channel_benchmarks FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_salary_data        ON salary_data        FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_compliance_rules   ON compliance_rules   FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_market_trends      ON market_trends      FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_vendor_profiles    ON vendor_profiles    FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_supply_repository  ON supply_repository  FOR ALL TO service_role USING (true) WITH CHECK (true);

-- ---------------------------------------------------------------------------
-- 5. TRIGGERS (auto-update updated_at)
-- ---------------------------------------------------------------------------

CREATE TRIGGER trg_knowledge_base_updated_at
    BEFORE UPDATE ON knowledge_base
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_channel_benchmarks_updated_at
    BEFORE UPDATE ON channel_benchmarks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_salary_data_updated_at
    BEFORE UPDATE ON salary_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_compliance_rules_updated_at
    BEFORE UPDATE ON compliance_rules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_vendor_profiles_updated_at
    BEFORE UPDATE ON vendor_profiles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_supply_repository_updated_at
    BEFORE UPDATE ON supply_repository
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
