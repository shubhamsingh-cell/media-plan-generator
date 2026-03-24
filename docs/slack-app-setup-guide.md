# Slack App Setup Guide for Nova AI Suite

## What the Code Expects

| Module | Purpose | Slack APIs Used |
|--------|---------|----------------|
| `nova_slack.py` | Interactive bot (@mentions, DMs, threads) | `auth.test`, `chat.postMessage`, `conversations.history`, `oauth.v2.access` |
| `slack_alerter.py` | Operational alerts (error spikes, health checks) | `chat.postMessage` + Incoming Webhook |
| `scripts/notify_slack.py` | CI/CD deploy notifications | `chat.postMessage` |

## Environment Variables Required (6 total)

| Env Var | Format | Used By | Required? |
|---------|--------|---------|-----------|
| `SLACK_BOT_TOKEN` | `xoxb-...` | All three modules | Yes (core) |
| `SLACK_SIGNING_SECRET` | Hex string | `nova_slack.py` (request verification) | Yes (security) |
| `SLACK_WEBHOOK_URL` | `https://hooks.slack.com/services/...` | `slack_alerter.py` | Recommended |
| `SLACK_ALERT_CHANNEL` | `#alerts` or channel ID | `slack_alerter.py`, `auto_qc.py` | Optional (defaults to `#alerts`) |
| `SLACK_CLIENT_ID` | String | `nova_slack.py` (token rotation) | Optional |
| `SLACK_CLIENT_SECRET` | String | `nova_slack.py` (token rotation) | Optional |

## Required OAuth Bot Scopes

```
app_mentions:read, chat:write, channels:history, channels:read,
im:history, im:read, im:write, users:read, incoming-webhook
```

## Required Bot Event Subscriptions

```
app_mention, message.im
```

## Step-by-Step Setup

### Step 1: Create the Slack App
1. Go to https://api.slack.com/apps
2. Click "Create New App" > "From scratch"
3. App Name: `Nova AI`, Workspace: Joveo
4. Click "Create App"

### Step 2: Configure Bot User
1. Sidebar > "App Home"
2. Set Display Name: `Nova`, Username: `nova-bot`
3. Enable "Messages Tab" + "Allow users to send messages"

### Step 3: Add OAuth Scopes
1. Sidebar > "OAuth & Permissions"
2. Under "Bot Token Scopes", add all 9 scopes listed above

### Step 4: Enable Event Subscriptions
1. Sidebar > "Event Subscriptions" > Toggle ON
2. Request URL: `https://media-plan-generator.onrender.com/api/slack/events`
3. Subscribe to bot events: `app_mention`, `message.im`

### Step 5: Enable Incoming Webhooks
1. Sidebar > "Incoming Webhooks" > Toggle ON
2. "Add New Webhook to Workspace" > Select `#alerts` channel

### Step 6: Install to Workspace
1. Sidebar > "Install App" > "Install to Workspace"
2. Copy Bot User OAuth Token (`xoxb-...`)
3. Copy Signing Secret from "Basic Information" > "App Credentials"

### Step 7: Create Channels
- `#alerts` -- Operational alerts
- `#nova-alerts` -- QC notifications
- Invite bot: `/invite @Nova` in each channel

### Step 8: Set Environment Variables on Render
```
SLACK_BOT_TOKEN=xoxb-your-token
SLACK_SIGNING_SECRET=your-signing-secret
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T.../B.../xxx
SLACK_ALERT_CHANNEL=#alerts
```

### Step 9: Verify
1. Visit: `https://media-plan-generator.onrender.com/api/slack/diagnostics`
2. In Slack: `@Nova hello` in `#alerts`
3. DM the bot directly
