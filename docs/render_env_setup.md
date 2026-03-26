# Render Environment Variables -- Slack & Google Calendar

Three env vars need to be added to the Render dashboard for the `media-plan-generator` service.

All three are **optional**. If unset, the corresponding features are silently disabled with debug-level log messages. The server will not crash.

---

## 1. SLACK_WEBHOOK_URL

**Used by:** `slack_alerts.py`, `slack_alerter.py`, `sentry_integration.py`, `alert_manager.py`

**Purpose:** Incoming webhook URL for Slack alert delivery (deploy notifications, error alerts, health check failures, self-healing escalations).

**Format:** `https://hooks.slack.com/services/T.../B.../xxx`

**How to get it:**
1. Go to https://api.slack.com/apps
2. Select your Slack app (or create one)
3. Navigate to Incoming Webhooks
4. Activate and create a webhook for your target channel (e.g., `#nova-alerts`)
5. Copy the webhook URL

**Fallback when unset:** All Slack alert functions return `False` and log a debug-level message. No errors.

**Render CLI:**
```bash
render env set SLACK_WEBHOOK_URL "https://hooks.slack.com/services/T.../B.../xxx" --service srv-...
```

---

## 2. GOOGLE_CALENDAR_CREDENTIALS

**Used by:** `calendar_sync.py`

**Purpose:** Google service account credentials for creating hiring campaign milestone events on Google Calendar.

**Format:** JSON string of a Google Cloud service account key file. Must contain `client_email`, `private_key`, and `token_uri` fields.

**How to get it:**
1. Go to Google Cloud Console > IAM & Admin > Service Accounts
2. Create a service account (or use existing)
3. Create a key (JSON format) -- downloads a `.json` file
4. Copy the entire JSON file contents as a single-line string
5. Share the target Google Calendar with the service account email (`client_email` from the JSON)

**Important:** The JSON must be a single-line string when set as an env var. Use:
```bash
cat service-account-key.json | jq -c .
```

**Fallback when unset:** `calendar_sync._is_available()` returns `False`. All public functions return `None` or `[]`. No errors.

**Render CLI:**
```bash
render env set GOOGLE_CALENDAR_CREDENTIALS '{"type":"service_account","project_id":"...","private_key_id":"...","private_key":"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n","client_email":"...@....iam.gserviceaccount.com","client_id":"...","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_x509_cert_url":"..."}' --service srv-...
```

**Dependency:** Requires the `cryptography` package (already in `requirements.txt`) for RS256 JWT signing.

---

## 3. GOOGLE_CALENDAR_ID

**Used by:** `calendar_sync.py`

**Purpose:** The Google Calendar ID to create events on.

**Format:** A calendar ID string. Examples:
- `primary` (the service account's own calendar -- default)
- `your-calendar-id@group.calendar.google.com` (a shared calendar)
- `someone@example.com` (a user's calendar, if shared with the service account)

**How to find it:**
1. Open Google Calendar web
2. Click the three dots next to the calendar name > Settings
3. Scroll to "Integrate calendar" section
4. Copy the "Calendar ID"

**Fallback when unset:** Defaults to `"primary"`. No errors.

**Render CLI:**
```bash
render env set GOOGLE_CALENDAR_ID "your-calendar-id@group.calendar.google.com" --service srv-...
```

---

## Verification

After setting the env vars on Render, verify they are active:

1. **Slack:** Hit `GET /api/slack/alerts/status` -- should show `"available": true`
2. **Calendar:** Hit `GET /api/calendar/events` -- should return events (or empty list if calendar is new)
3. **Health dashboard:** Check `/health-dashboard` -- Slack and Calendar modules should show as configured

## Local Development

Add to `~/.zshrc`:
```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/T.../B.../xxx"
export GOOGLE_CALENDAR_CREDENTIALS='{"type":"service_account",...}'
export GOOGLE_CALENDAR_ID="your-calendar-id@group.calendar.google.com"
```

Then `source ~/.zshrc`.
