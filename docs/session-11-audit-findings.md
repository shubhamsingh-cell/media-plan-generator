# Session 11 Audit Findings -- Action Items

## Supabase Critical Issues
1. [FIXED] nova_memory.py broken import (used non-existent get_supabase_client)
2. [TODO] 5 empty tables: salary_data, compliance_rules, market_trends, vendor_profiles, supply_repository
3. [TODO] nova_module_usage and nova_campaigns functions defined but never called
4. [TODO] Two separate connection strategies (raw REST vs SDK) -- unify
5. [TODO] No connection pooling (new TCP+TLS per request)
6. [TODO] Create nova_memory table in Supabase

## LLM Router Critical Issues
1. [TODO] OpenRouter rate limit cascade (7 variants share 1 key)
2. [TODO] xAI Grok mispriced as free (has $2/$10 per M token cost)
3. [TODO] full_plan_providers dead config (never used)
4. [TODO] ComplianceGuard uses wrong task type (verification vs compliance_check)
5. [TODO] Add Gemini to _FREE_TOOL_PROVIDERS for Nova tool calling
6. [TODO] Remove Moonshot from routing (key missing)
7. [TODO] Pass preferred_providers to call_llm_stream for Nova

## Stress Test Fixes Applied
1. [FIXED] RateLimiter unbounded memory growth (10K IP cap)
2. [FIXED] Null bytes pass through chat sanitizer
3. [FIXED] Audit log file write race condition
4. [FIXED] X-Forwarded-For rate limit bypass (rightmost IP)
5. [FIXED] JSON deep nesting RecursionError crash

## Session 12 Priority Queue
1. Populate 5 empty Supabase tables (create seeder scripts)
2. Fix OpenRouter shared rate limit
3. Move xAI to paid tier or add credit balance check
4. Wire ComplianceGuard to compliance_check task type
5. Add Gemini to Nova free tool providers
6. Complete app.py decomposition (chat POST routes)
7. Create nova_memory Supabase table
8. Unify Supabase connection strategy
