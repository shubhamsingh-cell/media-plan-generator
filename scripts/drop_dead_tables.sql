-- =============================================================================
-- Drop Dead Supabase Tables
-- Dead tables confirmed in S19, verified 100% unreferenced in S34
-- Zero Python code references, zero template references, zero runtime usage
--
-- Run manually via Supabase SQL Editor
-- Project: https://trpynqjatlhatxpzrvgt.supabase.co
-- Date: 2026-04-01
-- =============================================================================

-- 1. nova_avatars (from nova_schema_additions.sql)
--    Never used -- avatar display handled via CSS in chat UI
DROP TABLE IF EXISTS nova_avatars;

-- 2. nova_module_usage (from nova_platform_schema.sql)
--    Never populated -- analytics replaced by PostHog
DROP TABLE IF EXISTS nova_module_usage;

-- 3. nova_campaigns (from nova_platform_schema.sql)
--    Never populated -- campaign state stored client-side
DROP TABLE IF EXISTS nova_campaigns;

-- 4. research_cache (from scripts/supabase_schema.sql)
--    Superseded by the 'cache' table (L3 persistent cache)
DROP TABLE IF EXISTS research_cache;

-- Also drop the orphaned helper function that depended on nova_module_usage
DROP FUNCTION IF EXISTS nova_module_usage_stats(VARCHAR, TIMESTAMPTZ);
