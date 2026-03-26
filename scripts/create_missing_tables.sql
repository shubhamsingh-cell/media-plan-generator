-- ═══════════════════════════════════════════════════════════════════════════════
-- CREATE MISSING SUPABASE TABLES
-- Run this in Supabase SQL Editor to fix 404/400 errors
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── TABLE 1: cache (L3 persistent LLM response cache) ───────────────────────

CREATE TABLE IF NOT EXISTS public.cache (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key TEXT NOT NULL UNIQUE,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    expires_at TIMESTAMP WITH TIME ZONE,
    category TEXT DEFAULT 'general',
    hit_count INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_cache_key ON public.cache(key);
CREATE INDEX IF NOT EXISTS idx_cache_expires_at ON public.cache(expires_at);
CREATE INDEX IF NOT EXISTS idx_cache_category ON public.cache(category);
CREATE INDEX IF NOT EXISTS idx_cache_created_at ON public.cache(created_at);

ALTER TABLE public.cache ENABLE ROW LEVEL SECURITY;

CREATE POLICY IF NOT EXISTS cache_allow_all ON public.cache
    FOR ALL USING (true) WITH CHECK (true);

-- ─── TABLE 2: enrichment_log (data enrichment run history) ───────────────────

CREATE TABLE IF NOT EXISTS public.enrichment_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source TEXT NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    success BOOLEAN NOT NULL DEFAULT false,
    records INTEGER DEFAULT 0,
    metadata JSONB DEFAULT '{}'::jsonb,
    UNIQUE(source, started_at)
);

CREATE INDEX IF NOT EXISTS idx_enrichment_log_source ON public.enrichment_log(source);
CREATE INDEX IF NOT EXISTS idx_enrichment_log_started_at ON public.enrichment_log(started_at DESC);

ALTER TABLE public.enrichment_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY IF NOT EXISTS enrichment_log_allow_all ON public.enrichment_log
    FOR ALL USING (true) WITH CHECK (true);

-- ─── CLEANUP FUNCTIONS ───────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION cleanup_expired_cache() RETURNS void AS $$
BEGIN
    DELETE FROM public.cache WHERE expires_at IS NOT NULL AND expires_at < now();
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cleanup_old_enrichment_logs() RETURNS void AS $$
BEGIN
    DELETE FROM public.enrichment_log WHERE started_at < now() - interval '30 days';
END;
$$ LANGUAGE plpgsql;

-- ═══════════════════════════════════════════════════════════════════════════════
-- DONE. Supabase tables: 20 total (18 existing + cache + enrichment_log)
-- ═══════════════════════════════════════════════════════════════════════════════
