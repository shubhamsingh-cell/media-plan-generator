-- Supabase Cache Table Migration
-- Fixes: Table 'public.cache' not found (404 errors)
-- This creates the missing L3 cache layer for persistent LLM response caching

-- ═══════════════════════════════════════════════════════════════════════════════
-- CREATE CACHE TABLE
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS public.cache (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key TEXT NOT NULL UNIQUE,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    expires_at TIMESTAMP WITH TIME ZONE,
    category TEXT DEFAULT 'general',
    hit_count INTEGER DEFAULT 0
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- CREATE INDEXES FOR PERFORMANCE
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_cache_expires_at ON public.cache(expires_at);
CREATE INDEX IF NOT EXISTS idx_cache_category ON public.cache(category);
CREATE INDEX IF NOT EXISTS idx_cache_created_at ON public.cache(created_at);

-- ═══════════════════════════════════════════════════════════════════════════════
-- ENABLE ROW LEVEL SECURITY
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TABLE public.cache ENABLE ROW LEVEL SECURITY;

CREATE POLICY cache_allow_all ON public.cache
    FOR ALL
    USING (true)
    WITH CHECK (true);

-- ═══════════════════════════════════════════════════════════════════════════════
-- CREATE CLEANUP FUNCTION FOR EXPIRED ENTRIES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cleanup_expired_cache() RETURNS void AS $$
BEGIN
    DELETE FROM public.cache WHERE expires_at IS NOT NULL AND expires_at < now();
END;
$$ LANGUAGE plpgsql;
