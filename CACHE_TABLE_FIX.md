# Supabase Cache Table - Missing Table Error Fix

## Problem

When running the Media Plan Generator, you may encounter this error:

```
Table 'public.cache' not found
```

This error occurs when the `cache` table doesn't exist in your Supabase database. The application uses this table for L3 persistent caching (the `supabase_cache.py` module).

## Root Cause

The `cache` table is **not** created automatically during application startup. It must be manually created in your Supabase project via SQL.

## Solution

### Option 1: Automatic Verification Script (Recommended)

Run the Python script to verify and diagnose the issue:

```bash
python scripts/ensure_cache_table.py
```

This script will:
1. Check if the cache table exists
2. Verify all required columns are present
3. Test read/write permissions
4. Provide instructions if manual setup is needed

**Example output:**
```
Supabase Cache Table Initialization
======================================================================

Supabase URL: https://xxxxx.supabase.co...

Step 1: Checking if cache table exists...
Cache table NOT FOUND in Supabase

MANUAL SETUP REQUIRED:
1. Open Supabase SQL editor: https://xxxxx.supabase.co/sql/new
2. Copy and paste the SQL from scripts/cache_table_migration.sql
3. Click 'Run' to execute
```

### Option 2: Manual SQL Setup (Most Direct)

1. Open your Supabase project: https://app.supabase.com
2. Click **SQL Editor** in the left sidebar
3. Click **New Query**
4. Copy the entire content of `scripts/cache_table_migration.sql`
5. Paste it into the SQL editor
6. Click **Run** to execute
7. Wait for completion (should show "Query complete" with no errors)

### Option 3: Using Supabase CLI

If you have the Supabase CLI installed:

```bash
# Login to your Supabase account
supabase login

# Link your project
supabase link --project-ref <your-project-ref>

# Run the migration
supabase db push --file scripts/cache_table_migration.sql
```

## What Gets Created

The migration script creates:

### Table: `cache`

```sql
CREATE TABLE cache (
    key         TEXT        PRIMARY KEY,      -- Unique cache key
    data        JSONB       NOT NULL,         -- Cached data (JSON)
    created_at  TIMESTAMPTZ DEFAULT NOW(),   -- Creation timestamp
    expires_at  TIMESTAMPTZ NOT NULL,        -- Expiration timestamp (TTL)
    category    TEXT        DEFAULT 'general', -- Category tag
    hit_count   INTEGER     DEFAULT 0        -- Access counter
);
```

### Indexes

- `idx_cache_expires_at` - On `expires_at` column (used for cleanup)
- `idx_cache_category` - On `category` column (used for filtering)
- `idx_cache_created_at` - On `created_at` column (used for time queries)

### Row Level Security (RLS)

- RLS is **enabled** on the table (required for anon key access)
- Permissive policy: "Allow all operations on cache"
- This allows read/write via PostgREST while maintaining security

### Helper Function

- `cleanup_expired_cache()` - SQL function to delete expired entries
  - Called automatically by `supabase_cache.py` every 6 hours
  - Can also be called manually: `SELECT cleanup_expired_cache();`

## How the Cache Works

### Cache Hierarchy (3 Layers)

1. **L1: In-Memory Cache** (fastest, lost on restart)
   - Managed by `api_enrichment.py`
   - TTL: Per-request (short-lived)

2. **L2: Disk Cache** (survives restart, limited by disk)
   - Stored in `cache/` directory
   - TTL: Per-request

3. **L3: Supabase Persistent Cache** (survives deployments, shared)
   - Stored in `cache` table
   - Default TTL: 24 hours
   - Shared across all instances and deployments

### Cache Flow

When requesting data:
```
Request
  ↓
L1 (in-memory) → HIT? Return
  ↓ MISS
L2 (disk) → HIT? Return + add to L1
  ↓ MISS
L3 (Supabase) → HIT? Return + add to L1 + L2
  ↓ MISS
Fetch from API → Cache in L1, L2, L3
```

## Table Schema Details

### key (TEXT PRIMARY KEY)
- Unique identifier for cache entries
- Example: `"llm_response:abc123"`, `"news_feed:tech:2024"`
- Required, cannot be NULL

### data (JSONB NOT NULL)
- Cached data stored as JSON
- Can store any JSON-serializable data
- Example: `{"response": "...", "metadata": {...}}`
- Indexed for fast lookups

### created_at (TIMESTAMPTZ DEFAULT NOW())
- When the cache entry was created
- Automatically set to current UTC time
- Used for debugging and analytics

### expires_at (TIMESTAMPTZ NOT NULL)
- When the cache entry should be considered expired
- supabase_cache.py sets this based on `ttl_seconds` parameter
- Default TTL: 86400 seconds (24 hours)
- Queries filter out entries where `expires_at < NOW()`
- Cleanup function deletes entries where `expires_at < NOW()`

### category (TEXT DEFAULT 'general')
- Category tag for grouping and filtering cache entries
- Examples: `"llm"`, `"api"`, `"research"`, `"general"`
- Useful for purging entries by category
- Indexed for fast filtering

### hit_count (INTEGER DEFAULT 0)
- Counter incremented each time the cache entry is accessed
- Tracks popularity of cached items
- Updated in background (non-blocking, fire-and-forget)
- Useful for analytics and cache optimization

## Verification

After running the migration, verify the setup:

### Check Table Exists

```sql
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = 'cache';
```

Expected output: `cache` (1 row)

### Check Columns

```sql
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'cache'
ORDER BY ordinal_position;
```

Expected columns:
- `key` (text)
- `data` (jsonb)
- `created_at` (timestamp with time zone)
- `expires_at` (timestamp with time zone)
- `category` (text)
- `hit_count` (integer)

### Test Write

```sql
INSERT INTO cache (key, data, expires_at, category)
VALUES ('test_key', '{"test": true}'::jsonb, NOW() + INTERVAL '1 day', 'test');
```

### Test Read

```sql
SELECT key, category, hit_count FROM cache WHERE key = 'test_key';
```

Expected output: 1 row with your test data

### Check RLS is Enabled

```sql
SELECT schemaname, tablename, rowsecurity
FROM pg_tables
WHERE tablename = 'cache' AND schemaname = 'public';
```

Expected output: `rowsecurity = true`

## Environment Variables

Ensure these are set in your environment:

```bash
# .env or shell
export SUPABASE_URL=https://xxxxx.supabase.co
export SUPABASE_ANON_KEY=eyJhbGciOi...
```

Both are available in your Supabase project settings:
1. Open Supabase project
2. Click **Settings** → **API**
3. Copy the URL and anon key

## Troubleshooting

### Error: "new row violates row level security policy"

**Cause**: RLS policy is not properly configured.

**Fix**: Run the migration script again to ensure the policy exists:
```sql
CREATE POLICY IF NOT EXISTS "Allow all operations on cache"
    ON cache
    FOR ALL
    USING (true)
    WITH CHECK (true);
```

### Error: "relation 'cache' does not exist"

**Cause**: Table was not created.

**Fix**: Run the migration script (`scripts/cache_table_migration.sql`) in Supabase SQL editor.

### Error: "permission denied for schema public"

**Cause**: Your database user doesn't have CREATE permission.

**Fix**: Use the Supabase service role key or ensure your anon key has the required permissions.

### Cache operations timing out

**Cause**: Supabase connection issue or network latency.

**Fix**:
- Check `SUPABASE_URL` is correct
- Verify `SUPABASE_ANON_KEY` is valid
- Check Supabase project status at https://status.supabase.com
- supabase_cache.py has a 3-second HTTP timeout and will gracefully degrade to L1/L2 cache on timeout

## Performance Notes

- Cache table is write-heavy (every cache miss results in a write)
- Default cleanup runs every 6 hours (configurable in `app.py`)
- Each cache entry is small (~1KB average)
- With daily cleanup, table stays under 100KB for most applications
- Indexes keep queries fast even with thousands of entries

## Deployment

After creating the cache table:

1. Verify locally with `python scripts/ensure_cache_table.py`
2. Deploy your changes to production
3. The application will automatically use the cache table on startup

The `supabase_cache.py` module gracefully handles the cache table being unavailable (it logs a warning but continues working with L1/L2 cache only).

## Related Files

- `supabase_cache.py` - L3 cache implementation
- `api_enrichment.py` - Cache integration point
- `scripts/ensure_cache_table.py` - Verification script
- `scripts/cache_table_migration.sql` - Migration SQL
- `supabase_schema.sql` - Full schema (includes cache table)
