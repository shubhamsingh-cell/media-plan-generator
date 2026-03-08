# Nova Chatbot Architecture

Last updated: 2026-03-08

## System Overview

Nova is an AI-powered recruitment marketing chatbot embedded in the Media Plan Generator. It provides conversational access to Joveo's proprietary data (10,238+ publishers, 70+ countries) and 25 live API enrichment sources.

```
                        /api/chat (POST)
                             |
                     +-------v--------+
                     | handle_chat_   |
                     | request()      |
                     | (nova.py:3448) |
                     +-------+--------+
                             |
                     +-------v--------+
                     | Nova.chat()    |
                     | (nova.py:1928) |
                     +-------+--------+
                             |
              +--------------+--------------+
              |              |              |              |
         1. Learned     2. Response    3. Claude      4. Rule-based
            Answers        Cache         API            Fallback
          (0 tokens)    (0 tokens)   (Haiku 4.5)     (0 tokens)
              |              |              |              |
         Jaccard        Memory LRU     18 tools       Keyword
         similarity     + disk JSON    + tool loop     matching
         >= 0.35                       (max 8 iter)
```

## Decision Flow (in order)

### 1. Learned Answers (fastest, ~1ms)
- **File**: `nova.py`, `_check_learned_answers()` (line ~310)
- **Source**: 12 hardcoded Q&A pairs (`_PRELOADED_ANSWERS`) + `data/nova_learned_answers.json`
- **Matching**: Jaccard keyword similarity (threshold 0.35)
- **When it fires**: "what is joveo", "how many publishers", "what is CPC CPA CPH", etc.
- **Cost**: 0 API tokens

### 2. Response Cache (fast, ~2ms)
- **File**: `nova.py`, `_get_response_cache()` / `_set_response_cache()`
- **Storage**: Two-tier -- in-memory dict (200 entries, LRU) + disk (`data/nova_response_cache.json`)
- **TTL**: 7 days
- **Key normalization**: `_normalize_cache_key()` -- lowercase, strip punctuation, remove stop words, sort alphabetically
  - Example: "What's the CPC for healthcare?" and "healthcare CPC" both become key `"cpc healthcare"`
- **Eligibility**: Only standalone questions (conversation history <= 2 messages)
- **Write condition**: Claude API response with confidence >= 0.6
- **Cost**: 0 API tokens

### 3. Claude API (primary intelligence)
- **File**: `nova.py`, `_chat_with_claude()` (line ~1986)
- **Model**: `claude-haiku-4-5-20241022` ($1/$5 per M tokens)
- **Fallback model**: `claude-sonnet-4-20250514` ($3/$15 per M tokens) -- constant defined but requires manual switch
- **System prompt**: ~1,300 tokens (compressed)
- **Tools**: 18 tool definitions (~2,500 tokens compressed)
- **Prompt caching**: `anthropic-beta: prompt-caching-2024-07-31` header + `cache_control: ephemeral` on system prompt and last tool
- **Adaptive max_tokens**: 1024 (simple) / 2048 (medium) / 4096 (complex)
- **Tool loop**: Up to 8 iterations for complex multi-tool queries
- **History**: Last 6 turns (MAX_HISTORY_TURNS)
- **Cost**: ~2,000-8,000 tokens per request depending on complexity

### 4. Rule-Based Fallback (no API key or API failure)
- **File**: `nova.py`, `_chat_rule_based()` (line ~2164)
- **How**: Keyword detection -> direct data source queries -> formatted response
- **When**: No `ANTHROPIC_API_KEY` set, or Claude API throws exception
- **Cost**: 0 API tokens

## Key Files

| File | Lines | Purpose |
|------|-------|---------|
| `nova.py` | ~3,500 | Core chatbot: learned answers, caching, Claude API, rule-based, 18 tools, data loading |
| `nova_slack.py` | ~900 | Slack bot adapter: event handling, token rotation, formats for Slack mrkdwn |
| `app.py` | ~8,300 | HTTP server: routes /api/chat, /api/nova/metrics, /api/generate, static files |
| `api_enrichment.py` | ~1,200 | 25 external API integrations with two-tier caching |
| `monitoring.py` | ~300 | Health checks, request metrics, MetricsCollector singleton |

## Data Files

| File | Purpose |
|------|---------|
| `data/nova_learned_answers.json` | 12 Q&A pairs (shared with Slack bot) |
| `data/nova_response_cache.json` | Disk-persisted response cache (auto-created) |
| `data/joveo_publishers.json` | Publisher catalog (10,238+ entries) |
| `data/joveo_channels.json` | Channel/platform catalog |
| `data/joveo_global_supply.json` | Country-level supply data |
| `data/recruitment_benchmarks.json` | CPC/CPA/CPH benchmarks |
| `data/recruitment_benchmarks_deep.json` | Deep industry benchmarks |
| `data/regional_hiring_intelligence.json` | Regional hiring strategies |
| `data/industry_white_papers.json` | Industry knowledge base |

## API Endpoints

| Endpoint | Method | Auth | Purpose |
|----------|--------|------|---------|
| `/api/chat` | POST | none | Main chatbot endpoint |
| `/api/nova/chat` | POST | none | Alias for /api/chat |
| `/api/nova/metrics` | GET | admin | Nova performance metrics (tokens, cache hits, cost) |
| `/api/health` | GET | none | Liveness probe |
| `/api/health/ready` | GET | none | Readiness probe |
| `/api/metrics` | GET | admin | Server-wide metrics |
| `/api/generate` | POST | none | Media plan generation |
| `/api/slack/events` | POST | Slack | Slack event webhook |

## Metrics Tracking

Nova tracks per-request metrics via `_NovaMetrics` singleton in `nova.py`:

- **Response mode counters**: learned_answers, cache_hits, claude_api, rule_based
- **Token usage**: input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens
- **Latency**: Per-request ms (rolling window of last 200)
- **Estimated cost**: Calculated from Haiku 4.5 pricing ($1/$5 per M tokens)
- **Cache hit rate**: (learned + cache) / total * 100

Access via `GET /api/nova/metrics` (requires `X-Admin-Key` header).

## Token Budget per Request

| Component | Tokens | Notes |
|-----------|--------|-------|
| System prompt | ~1,300 | Compressed, cached after first request |
| Tool definitions | ~2,500 | 18 tools, compressed, cached |
| History (6 turns) | ~800-1,600 | Depends on conversation length |
| User message | ~20-100 | |
| **Total input** | **~4,600-5,500** | First request; ~600-1,700 after prompt caching |
| **Output** | **1,024-4,096** | Adaptive based on query complexity |

## Testing

Run the automated test suite:
```bash
# Against local server
./tests/test_nova_chat.sh

# Against production
./tests/test_nova_chat.sh https://media-plan-generator.onrender.com

# With metrics endpoint testing
ADMIN_API_KEY=your_key ./tests/test_nova_chat.sh https://media-plan-generator.onrender.com
```

Tests cover: response structure, learned answers, cache behavior, ask-before-answering logic, complex queries, empty input handling, and metrics endpoint.
