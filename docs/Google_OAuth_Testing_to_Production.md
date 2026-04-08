# Google OAuth: Testing to Production Migration

## Problem

Collin (and any non-test-user) gets a 500 error when trying to sign in with Google.
This happens because the GCP OAuth consent screen is in **Testing** mode, which restricts
sign-in to a maximum of 100 explicitly listed test users.

## Root Causes Found and Fixed (Code)

### 1. Missing `/api/config` endpoint (CRITICAL)

The frontend (`nova-auth-gate.js`, `body_auth_js.html`, `hub.html`, `nova.html`) fetches
`/api/config` to get the Supabase URL and anon key for initializing Google OAuth. This
endpoint had no handler in `app.py`, causing a 404. Without the config, NovaAuth never
initialized, and the auth gate's login button showed "Failed to connect."

**Fix**: Added `GET /api/config` handler in `app.py` that returns:
```json
{
  "auth_enabled": true,
  "supabase_url": "https://xxx.supabase.co",
  "supabase_anon_key": "eyJ...",
  "allowed_domains": ["joveo.com"],
  "provider": "google"
}
```

### 2. No OAuth error detection or display

When Google OAuth fails (e.g., user not in test list, access denied), Supabase redirects
back with `error` and `error_description` in the URL. The auth gate silently swallowed
these errors, showing the login screen again with no explanation.

**Fix**: Added OAuth error detection in `nova-auth-gate.js` that:
- Parses `error` and `error_description` from URL hash/query params
- Maps error codes to user-friendly messages
- Displays a red error box in the auth gate UI
- Cleans the URL after capturing the error

### 3. Improved error messages in `nova-auth.js`

Enhanced the `signInWithGoogle()` function with better error messages for common
OAuth failure scenarios (access_denied, unauthorized).

## Manual GCP Console Steps Required

### Option A: Move to Production (Recommended)

This removes the 100-user test limit. Required for general @joveo.com access.

1. Go to [GCP Console](https://console.cloud.google.com/) > Select project `gen-lang-client-0603536849`
2. Navigate to **APIs & Services** > **OAuth consent screen**
3. Current status will show: **Testing** (with a blue "Testing" badge)
4. Click **PUBLISH APP** button
5. Review the confirmation dialog:
   - If the app only uses non-sensitive scopes (email, profile, openid), it will be
     approved immediately with no review
   - If it uses sensitive or restricted scopes, Google will require verification
     (this can take days/weeks)
6. The scopes Nova uses are:
   - `email` (non-sensitive)
   - `profile` (non-sensitive)
   - `openid` (non-sensitive)
   These should auto-approve without review.
7. After publishing, verify the status shows **In production**

### Option B: Add Users to Test List (Quick Workaround)

If you cannot publish to production yet:

1. Go to **APIs & Services** > **OAuth consent screen**
2. Scroll down to **Test users** section
3. Click **+ ADD USERS**
4. Add Collin's @joveo.com email address
5. Click **Save**

Note: Maximum 100 test users allowed.

### Verify Redirect URIs

While in the GCP Console, verify redirect URIs are configured:

1. Go to **APIs & Services** > **Credentials**
2. Click on the OAuth 2.0 Client ID used by Supabase
3. Under **Authorized redirect URIs**, ensure these are listed:
   - `https://<your-supabase-project>.supabase.co/auth/v1/callback`
   (Supabase handles the OAuth callback, not the Nova app directly)

### Verify Supabase Google Provider Config

1. Go to [Supabase Dashboard](https://supabase.com/dashboard)
2. Select the Nova project
3. Go to **Authentication** > **Providers** > **Google**
4. Verify:
   - Google provider is **enabled**
   - Client ID matches the GCP OAuth client ID
   - Client Secret is set
   - Authorized Client IDs includes the same client ID

## Environment Variables Required (Render)

Both of these must be set on Render for OAuth to work:

| Variable | Description | Status |
|----------|-------------|--------|
| `SUPABASE_URL` | Supabase project URL (e.g., `https://xxx.supabase.co`) | Should be set (56 vars on Render) |
| `SUPABASE_ANON_KEY` | Supabase anonymous/public key | Should be set |

## How the OAuth Flow Works

```
1. User clicks "Sign in with Google" on auth gate
2. Frontend fetches GET /api/config to get Supabase URL + anon key
3. Frontend redirects to: {supabase_url}/auth/v1/authorize?provider=google&redirect_to={page}
4. Supabase redirects to Google OAuth consent screen
5. Google authenticates user, redirects back to Supabase callback
6. Supabase redirects back to Nova with access_token in URL hash
7. nova-auth-gate.js extracts JWT, verifies @joveo.com domain
8. nova-auth.js initializes Supabase client, sets up auth listener
9. Session is stored in localStorage for persistence
```

If step 5 fails (testing mode, user not in list), Google returns an error to Supabase,
which passes it back as `error=access_denied` in the redirect URL. The new error handling
code captures and displays this to the user.

## Files Changed

- `app.py` -- Added `GET /api/config` endpoint (lines 9799-9815)
- `auth.py` -- Added `/api/config` to PUBLIC_ENDPOINTS
- `static/nova-auth-gate.js` -- OAuth error detection + user-friendly error display
- `static/nova-auth.js` -- Improved error messages in signInWithGoogle()
