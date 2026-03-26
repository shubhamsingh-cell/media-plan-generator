-- Nova AI Suite -- Supabase Schema
-- Run this in Supabase SQL Editor to create all tables
-- Project: https://trpynqjatlhatxpzrvgt.supabase.co
-- Created: 2026-03-23

-- =========================================================================
-- 1. Knowledge Base (general industry insights, benchmarks, strategies)
-- =========================================================================
CREATE TABLE IF NOT EXISTS knowledge_base (
    id BIGSERIAL PRIMARY KEY,
    category TEXT NOT NULL,           -- 'industry_insights', 'platform_data', 'benchmarks', etc.
    key TEXT NOT NULL,                -- lookup key (e.g., 'technology', 'healthcare')
    data JSONB NOT NULL,             -- the actual data blob
    source TEXT DEFAULT 'manual',    -- 'manual', 'api', 'firecrawl', 'migration'
    version INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(category, key)
);

-- =========================================================================
-- 2. Channel Benchmarks (CPC, CPA, apply rates per channel per industry)
-- =========================================================================
CREATE TABLE IF NOT EXISTS channel_benchmarks (
    id BIGSERIAL PRIMARY KEY,
    channel TEXT NOT NULL,            -- 'indeed', 'linkedin', 'google_ads', etc.
    industry TEXT DEFAULT 'overall',  -- 'technology', 'healthcare', etc.
    cpc NUMERIC(10,2),
    cpa NUMERIC(10,2),
    apply_rate NUMERIC(6,4),
    quality_score NUMERIC(4,2),
    monthly_reach BIGINT,
    pricing_model TEXT,
    data_source TEXT DEFAULT 'benchmark', -- 'benchmark', 'live_firecrawl', 'api'
    metadata JSONB DEFAULT '{}',
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(channel, industry)
);

-- =========================================================================
-- 3. Salary Data (compensation by role, location, industry)
-- =========================================================================
CREATE TABLE IF NOT EXISTS salary_data (
    id BIGSERIAL PRIMARY KEY,
    role TEXT NOT NULL,
    location TEXT DEFAULT 'national',
    industry TEXT DEFAULT 'overall',
    median_salary NUMERIC(12,2),
    salary_10th NUMERIC(12,2),
    salary_25th NUMERIC(12,2),
    salary_75th NUMERIC(12,2),
    salary_90th NUMERIC(12,2),
    soc_code TEXT,
    data_source TEXT DEFAULT 'bls',
    metadata JSONB DEFAULT '{}',
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(role, location, industry)
);

-- =========================================================================
-- 4. Compliance Rules (pay transparency laws, EEO requirements)
-- =========================================================================
CREATE TABLE IF NOT EXISTS compliance_rules (
    id BIGSERIAL PRIMARY KEY,
    rule_type TEXT NOT NULL,          -- 'pay_transparency', 'bias_language', 'eeo', 'ofccp'
    jurisdiction TEXT DEFAULT 'federal', -- 'federal', 'california', 'new_york_city', 'eu', etc.
    rule_name TEXT NOT NULL,
    description TEXT,
    effective_date DATE,
    status TEXT DEFAULT 'active',     -- 'active', 'upcoming', 'expired'
    keywords JSONB DEFAULT '[]',     -- trigger words/phrases
    suggested_fix TEXT,
    severity TEXT DEFAULT 'warning',  -- 'critical', 'warning', 'info'
    metadata JSONB DEFAULT '{}',
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(rule_type, jurisdiction, rule_name)
);

-- =========================================================================
-- 5. Market Trends (recruitment news, industry trends)
-- =========================================================================
CREATE TABLE IF NOT EXISTS market_trends (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    summary TEXT,
    source TEXT,
    url TEXT,
    category TEXT DEFAULT 'general',  -- 'cpc_trends', 'hiring_volume', 'industry_news'
    published_date DATE,
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    metadata JSONB DEFAULT '{}',
    UNIQUE(url)
);

-- =========================================================================
-- 6. Vendor Profiles (job board vendor data)
-- =========================================================================
CREATE TABLE IF NOT EXISTS vendor_profiles (
    id BIGSERIAL PRIMARY KEY,
    vendor_name TEXT NOT NULL UNIQUE,
    domain TEXT,
    category TEXT,                    -- 'major_job_board', 'programmatic', 'social', 'niche'
    pricing JSONB DEFAULT '{}',      -- {monthly_cost, cpc_range, free_tier, etc.}
    features JSONB DEFAULT '[]',
    coverage JSONB DEFAULT '{}',     -- {countries, industries, role_types}
    performance JSONB DEFAULT '{}',  -- {avg_cpc, avg_cpa, apply_rate by industry}
    data_source TEXT DEFAULT 'manual',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- =========================================================================
-- 7. Supply Repository (Joveo publisher/channel data)
-- =========================================================================
CREATE TABLE IF NOT EXISTS supply_repository (
    id BIGSERIAL PRIMARY KEY,
    publisher_name TEXT NOT NULL,
    publisher_id TEXT,
    category TEXT,                    -- 'job_board', 'social', 'programmatic', 'niche'
    countries JSONB DEFAULT '[]',
    industries JSONB DEFAULT '[]',
    performance JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(publisher_name)
);

-- =========================================================================
-- 8a. Cache (L3 persistent LLM response cache for supabase_cache.py)
-- =========================================================================
CREATE TABLE IF NOT EXISTS cache (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key TEXT NOT NULL UNIQUE,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    category TEXT DEFAULT 'general',
    hit_count INTEGER DEFAULT 0
);

-- =========================================================================
-- 8b. Research Cache (web research results with timestamps)
-- =========================================================================
CREATE TABLE IF NOT EXISTS research_cache (
    id BIGSERIAL PRIMARY KEY,
    query_hash TEXT NOT NULL UNIQUE,
    query TEXT NOT NULL,
    results JSONB NOT NULL,
    source TEXT DEFAULT 'firecrawl',  -- 'firecrawl', 'api', 'manual'
    ttl_hours INTEGER DEFAULT 24,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);

-- =========================================================================
-- 9. Enrichment Log (audit trail for data updates)
-- =========================================================================
CREATE TABLE IF NOT EXISTS enrichment_log (
    id BIGSERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    action TEXT NOT NULL,             -- 'insert', 'update', 'refresh', 'migrate'
    records_affected INTEGER DEFAULT 0,
    source TEXT,                      -- 'bls_api', 'firecrawl', 'migration', etc.
    details JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =========================================================================
-- Indexes for performance
-- =========================================================================
CREATE INDEX IF NOT EXISTS idx_kb_category ON knowledge_base(category);
CREATE INDEX IF NOT EXISTS idx_kb_updated ON knowledge_base(updated_at);
CREATE INDEX IF NOT EXISTS idx_benchmarks_channel ON channel_benchmarks(channel);
CREATE INDEX IF NOT EXISTS idx_benchmarks_industry ON channel_benchmarks(industry);
CREATE INDEX IF NOT EXISTS idx_salary_role ON salary_data(role);
CREATE INDEX IF NOT EXISTS idx_salary_location ON salary_data(location);
CREATE INDEX IF NOT EXISTS idx_compliance_type ON compliance_rules(rule_type);
CREATE INDEX IF NOT EXISTS idx_compliance_jurisdiction ON compliance_rules(jurisdiction);
CREATE INDEX IF NOT EXISTS idx_trends_category ON market_trends(category);
CREATE INDEX IF NOT EXISTS idx_trends_date ON market_trends(published_date DESC);
CREATE INDEX IF NOT EXISTS idx_enrichment_table ON enrichment_log(table_name);
CREATE INDEX IF NOT EXISTS idx_enrichment_date ON enrichment_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_cache_key ON cache(key);
CREATE INDEX IF NOT EXISTS idx_cache_expires_at ON cache(expires_at);
CREATE INDEX IF NOT EXISTS idx_cache_category ON cache(category);
CREATE INDEX IF NOT EXISTS idx_research_expires ON research_cache(expires_at);

-- =========================================================================
-- Row Level Security (RLS) -- enable for all tables
-- Using permissive policies for server-side anon key access
-- =========================================================================
ALTER TABLE knowledge_base ENABLE ROW LEVEL SECURITY;
ALTER TABLE channel_benchmarks ENABLE ROW LEVEL SECURITY;
ALTER TABLE salary_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE compliance_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE market_trends ENABLE ROW LEVEL SECURITY;
ALTER TABLE vendor_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE supply_repository ENABLE ROW LEVEL SECURITY;
ALTER TABLE cache ENABLE ROW LEVEL SECURITY;
ALTER TABLE research_cache ENABLE ROW LEVEL SECURITY;
ALTER TABLE enrichment_log ENABLE ROW LEVEL SECURITY;

-- Permissive policies (server-side only, anon key)
CREATE POLICY "Allow all on knowledge_base" ON knowledge_base FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all on channel_benchmarks" ON channel_benchmarks FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all on salary_data" ON salary_data FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all on compliance_rules" ON compliance_rules FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all on market_trends" ON market_trends FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all on vendor_profiles" ON vendor_profiles FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all on supply_repository" ON supply_repository FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all on cache" ON cache FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all on research_cache" ON research_cache FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all on enrichment_log" ON enrichment_log FOR ALL USING (true) WITH CHECK (true);

-- =========================================================================
-- Auto-update updated_at trigger
-- =========================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_updated_at_knowledge_base
    BEFORE UPDATE ON knowledge_base
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER set_updated_at_channel_benchmarks
    BEFORE UPDATE ON channel_benchmarks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER set_updated_at_salary_data
    BEFORE UPDATE ON salary_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER set_updated_at_compliance_rules
    BEFORE UPDATE ON compliance_rules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER set_updated_at_vendor_profiles
    BEFORE UPDATE ON vendor_profiles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER set_updated_at_supply_repository
    BEFORE UPDATE ON supply_repository
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
