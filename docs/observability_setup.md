# Nova AI Suite -- LLM Observability Setup

End-to-end deployment guide for the **Langfuse + LiteLLM** observability
stack. Everything below is OSS / self-hosted; no third-party LLM telemetry.

| Layer        | Tool       | Role                                   |
|--------------|------------|----------------------------------------|
| Errors       | Sentry     | Already in production                  |
| Product      | PostHog    | Already in production                  |
| **LLM trace**| **Langfuse**| **NEW** -- per-call tracing + analytics |
| **Gateway** | **LiteLLM**| **OPTIONAL** -- unified OpenAI gateway  |

Files in this stack (all in repo root unless noted):

- `docker-compose.langfuse.yml` -- Langfuse v3 + ClickHouse + Postgres + Redis + MinIO
- `litellm_proxy.docker-compose.yml` -- LiteLLM Proxy (single service)
- `litellm_config.yaml` -- LiteLLM model routes + Langfuse callback
- `langfuse_integration.py` -- stdlib-only fire-and-forget tracer
- `tests/test_langfuse_integration.py` -- unit tests for the tracer

---

## 1. Deploy Langfuse

You have two paths -- pick one.

### 1a. Local Docker (development)

```bash
# 1. Generate secrets (do NOT commit these)
export NEXTAUTH_SECRET="$(openssl rand -base64 32)"
export SALT="$(openssl rand -base64 32)"
export ENCRYPTION_KEY="$(openssl rand -hex 32)"          # exactly 64 hex chars
export POSTGRES_PASSWORD="$(openssl rand -base64 24)"
export CLICKHOUSE_PASSWORD="$(openssl rand -base64 24)"
export REDIS_PASSWORD="$(openssl rand -base64 24)"
export MINIO_ROOT_PASSWORD="$(openssl rand -base64 24)"
export NEXTAUTH_URL="http://localhost:3000"
export LANGFUSE_INIT_USER_PASSWORD="$(openssl rand -base64 18)"

# 2. Verify the compose file parses cleanly
docker compose -f docker-compose.langfuse.yml config > /dev/null && echo OK

# 3. Bring it up
docker compose -f docker-compose.langfuse.yml up -d

# 4. Watch the boot (langfuse-web takes 30-60s to migrate)
docker compose -f docker-compose.langfuse.yml logs -f langfuse-web

# 5. Open the dashboard
open http://localhost:3000   # macOS
# Sign in with admin@joveo.com / $LANGFUSE_INIT_USER_PASSWORD
```

After login: **Settings -> API Keys -> Create new key**. Copy the public
(`pk-lf-...`) and secret (`sk-lf-...`).

### 1b. Render Private Service (production)

Langfuse on Render is best deployed as **separate services** (Render does
not run multi-container compose stacks natively).

1. **ClickHouse** -- create a new "Private Service" with image
   `clickhouse/clickhouse-server:24.8`.
   - Disk: 10 GB on `/var/lib/clickhouse`.
   - Env vars: `CLICKHOUSE_DB=default`, `CLICKHOUSE_USER=langfuse`,
     `CLICKHOUSE_PASSWORD=<generated>`,
     `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`.
   - Plan: minimum **Standard 4GB** -- ClickHouse OOMs on 2GB once you
     have a few thousand traces.
2. **Postgres** -- use Render Postgres (Standard tier). Save the
   connection string as `LANGFUSE_DATABASE_URL`.
3. **Redis** -- use Render Key-Value (Redis-compatible) or stand up your
   own private service from `redis:7-alpine` with a password.
4. **MinIO -> swap for S3** -- on Render, replace MinIO with a real
   S3-compatible bucket. Recommended: **Cloudflare R2** ($0 egress).
   Set `LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT` to the R2 endpoint.
5. **langfuse-web** -- Web Service (private), image `langfuse/langfuse:3`.
   Map env vars from `docker-compose.langfuse.yml` to the live secrets
   above. Set `NEXTAUTH_URL` to the public Render URL or your custom
   domain (e.g. `https://obs.joveo.com`).
6. **langfuse-worker** -- Background Worker, image
   `langfuse/langfuse-worker:3`, same env as `langfuse-web`.

Smoke test once it's up:

```bash
curl -fsS https://obs.joveo.com/api/public/health
# {"status":"OK","version":"3.x.x"}
```

---

## 2. Wire `langfuse_integration.py` into `llm_router.py`

The integration is designed to be **non-invasive**. We import the function
at the bottom of `call_llm` (after the result is built) and let it
fire-and-forget. Failures are logged, never raised.

### 2.1 Set env vars on Render

Add these via `scripts/set_render_env.sh` or the Render dashboard:

```bash
LANGFUSE_HOST=https://obs.joveo.com         # NO trailing slash
LANGFUSE_PUBLIC_KEY=pk-lf-...               # from Langfuse UI
LANGFUSE_SECRET_KEY=sk-lf-...               # from Langfuse UI
LANGFUSE_SAMPLE_RATE=1.0                    # start at 1.0; tune down if needed
LANGFUSE_ENABLED=true                       # kill switch
```

If any of `LANGFUSE_HOST` / `LANGFUSE_PUBLIC_KEY` / `LANGFUSE_SECRET_KEY`
is missing, the tracer is a silent no-op -- safe to deploy before
Langfuse is live.

### 2.2 Wiring snippet (5 lines)

Open `llm_router.py` and find the `return result` at the end of
`call_llm`. Add the import once near the top of the module, and the call
just before the return. **Do not modify `call_llm`'s return shape.**

```python
# Top of llm_router.py (next to the other imports)
from langfuse_integration import trace_llm_call

# At the end of call_llm(), right before `return result`:
trace_llm_call(
    model=result.get("model") or "",
    input_messages=messages,
    output=result.get("text"),
    latency_ms=float(result.get("latency_ms") or 0.0),
    metadata={
        "provider": result.get("provider"),
        "provider_name": result.get("provider_name"),
        "task_type": result.get("task_type"),
        "input_tokens": result.get("input_tokens"),
        "output_tokens": result.get("output_tokens"),
        "cache_hit": result.get("cache_hit"),
        "fallback_used": result.get("fallback_used"),
    },
    error=result.get("error"),
)
return result
```

That's the entire integration.

### 2.3 Multi-turn sessions

For Nova chat, pass `session_id` so multi-turn chats group inside
Langfuse:

```python
trace_llm_call(
    ...,
    user_id=session.get("user_email") or "",
    session_id=session.get("conversation_id") or "",
)
```

`conversation_id` is already plumbed through `nova.py` and persisted in
`nova_conversations`, so this is a one-line change at each call site.

---

## 3. (Optional) Deploy LiteLLM Proxy

LiteLLM gives you a **single OpenAI-format endpoint** for every provider
plus **automatic Langfuse tracing** for free.

Joveo doesn't *need* it -- `llm_router.py` already does smart routing --
but it's nice for:
- Internal tools / scripts that want one URL.
- Built-in cost guardrails (`max_budget` in `litellm_config.yaml`).
- Forcing every call through Langfuse without touching application code.

### 3.1 Local dev

```bash
export ANTHROPIC_API_KEY=...
export GEMINI_API_KEY=...
export OPENAI_API_KEY=...
export VOYAGE_API_KEY=...
export LITELLM_MASTER_KEY="$(openssl rand -hex 24)"
export LANGFUSE_HOST=http://host.docker.internal:3000
export LANGFUSE_PUBLIC_KEY=pk-lf-...
export LANGFUSE_SECRET_KEY=sk-lf-...

docker compose -f litellm_proxy.docker-compose.yml config > /dev/null && echo OK
docker compose -f litellm_proxy.docker-compose.yml up -d

# Smoke test
curl -fsS http://localhost:4000/health/liveliness

curl -sS http://localhost:4000/v1/chat/completions \
  -H "Authorization: Bearer $LITELLM_MASTER_KEY" \
  -H "Content-Type: application/json" \
  -d '{
        "model":"claude-haiku-4-5",
        "messages":[{"role":"user","content":"ping"}]
      }' | jq .
```

The request should appear in Langfuse within ~5 seconds.

### 3.2 Render deploy

1. Create a Render **Private Service**, image `ghcr.io/berriai/litellm:main-latest`.
2. Upload `litellm_config.yaml` as a **Secret File** at `/app/config.yaml`.
3. Start command: `--config /app/config.yaml --port 4000 --num_workers 2`.
4. Set the env vars from `litellm_proxy.docker-compose.yml`.
5. Health check path: `/health/liveliness`.

Then, optionally, point one or two Nova call sites at it instead of the
direct provider SDK:

```python
import urllib.request, json, os

req = urllib.request.Request(
    f"{os.environ['LITELLM_PROXY_URL']}/v1/chat/completions",
    data=json.dumps({
        "model": "claude-haiku-4-5",
        "messages": messages,
    }).encode("utf-8"),
    headers={
        "Authorization": f"Bearer {os.environ['LITELLM_MASTER_KEY']}",
        "Content-Type": "application/json",
    },
    method="POST",
)
```

---

## 4. Gradual rollout

A safe rollout sequence:

1. **Deploy Langfuse** (Step 1) and confirm `/api/public/health` returns OK.
2. **Set env vars** with `LANGFUSE_SAMPLE_RATE=0.0`. The tracer reads them
   but skips every trace -- zero risk.
3. **Add the wiring snippet** to `llm_router.py`. Deploy. Logs should be
   clean; no extra latency.
4. **Bump `LANGFUSE_SAMPLE_RATE` to 0.05** -- 5 % sampling. Watch
   Langfuse for traces and watch Sentry / app logs for any new warnings.
5. **Bump to 1.0** once you're satisfied. The tracer's daemon thread
   adds ~0.1 ms to the LLM call site (just enqueueing the thread).

The `LANGFUSE_ENABLED=false` env var is a global kill switch you can
flip during incidents.

---

## 5. Troubleshooting

| Symptom                                           | Likely cause                                                    | Fix                                                                                     |
|---------------------------------------------------|------------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| `langfuse-web` keeps restarting, OOM in logs      | ClickHouse OOM-killed; dragging the web app down                 | Bump ClickHouse to 4GB (Render Standard 4GB), reduce `mark_cache_size`                  |
| `Permission denied` in MinIO logs                 | Volume mounted with wrong UID                                    | `chown -R 1000:1000 ./langfuse_minio_data` or recreate the volume                       |
| Login redirects to `localhost:3000` from prod URL | `NEXTAUTH_URL` wrong                                             | Set `NEXTAUTH_URL` to the **public** URL of `langfuse-web` exactly                      |
| 401 on POST `/api/public/ingestion`               | Public/secret key swapped or stale                               | Re-copy from Langfuse UI; restart Nova so `_get_config()` reloads                       |
| No traces appearing despite 200 OK                | `LANGFUSE_SAMPLE_RATE=0.0` or worker not running                 | Check `LANGFUSE_SAMPLE_RATE`, then `docker compose logs langfuse-worker`                |
| ClickHouse migration fails on first boot          | Stale volume from older Langfuse v2                              | Drop `langfuse_clickhouse_data` and let v3 re-bootstrap                                 |
| LiteLLM proxy returns `unsupported_value`         | Provider rejects a param LiteLLM forwarded                       | Already handled: `drop_params: true` in `litellm_config.yaml` -- restart proxy          |
| Render `langfuse-web` 502 for first 60s           | Cold start + Postgres migrations                                 | Increase Render health-check `start_period` to 90s                                      |
| Traces missing user_id                            | Caller didn't pass `user_id` / `session_id`                      | Plumb `session.get("user_email")` and `conversation_id` into the `trace_llm_call(...)`  |

### Quick verification commands

```bash
# Is the tracer enabled?
python3 -c "from langfuse_integration import is_enabled, health_fingerprint; \
            print('enabled:', is_enabled()); print('fingerprint:', health_fingerprint())"

# Send a synthetic trace
python3 -c "from langfuse_integration import trace_llm_call; \
            trace_llm_call(model='manual-test', input_messages=[{'role':'user','content':'hi'}], \
                           output='ok', latency_ms=42.0); \
            import time; time.sleep(2)"

# Tail Langfuse worker logs
docker compose -f docker-compose.langfuse.yml logs -f langfuse-worker | grep -E 'INFO|WARN|ERROR'
```

---

## 6. Cost & retention notes

- ClickHouse stores raw events; default Langfuse retention is **forever**.
  Add a TTL via the `LANGFUSE_DATA_RETENTION_DAYS` env var on
  `langfuse-web` if you want auto-pruning (recommended: 90 days).
- MinIO blob storage grows with large prompts/outputs. On Render, swap
  for Cloudflare R2 (~ $15 / TB / month, free egress).
- LiteLLM's `max_budget: 100` in `litellm_config.yaml` is a hard daily
  USD ceiling -- update it as Joveo's traffic grows.
