# Nova Chatbot -- Deep Stress Test & Architecture Audit
**Date:** 2026-03-24

## 8 CRITICAL Issues (must fix)

| # | Issue | Impact | File |
|---|-------|--------|------|
| C-01 | All LLM providers fail = silent empty response | User sees blank message | nova.py:9610 |
| C-02 | Serial API enrichment adds 10-30s blocking latency | 6 APIs called sequentially before LLM starts | app.py:22030 |
| C-03 | ~500 lines duplicated enrichment code (chat vs stream) | Maintenance nightmare, divergence risk | app.py:22010-22528 |
| C-04 | Data loss: response sent before Supabase write | Crash during daemon thread = lost conversation | app.py:22257 |
| C-05 | History cap too low: only 6 turns sent to LLM | LLM loses context after 3 user questions | nova.py:108 |
| C-06 | No input length cap on current message | 50KB paste goes straight to LLM | nova.py:9594 |
| C-07 | DOM thrash: full innerHTML rewrite per streamed word | 500 DOM rebuilds per response | nova-chat.js:1277 |
| C-08 | Supabase failures silently swallowed, no retry | Data loss when Supabase is down | app.py:16004 |

## 13 WARNINGS (should fix)

| # | Issue | File |
|---|-------|------|
| W-01 | Streaming is simulated (full response then word-by-word) | app.py:22553 |
| W-02 | Enter key race condition (microsecond window) | nova-chat.js:1192 |
| W-03 | Client 60s timeout races server 60s LLM budget | nova-chat.js:1224 |
| W-04 | No client-side message length validation | nova-chat.js:1188 |
| W-05 | Two incompatible Supabase persistence schemas | app.py vs nova_persistence.py |
| W-06 | No token counting / context window management | nova.py:108 |
| W-07 | User input not sanitized for prompt injection | nova.py:9594 |
| W-08 | Markdown renderer doesn't validate URL protocols (XSS) | nova-chat.js:401 |
| W-09 | Canvas 3D orb runs at 60fps when panel closed | nova-chat.js:537 |
| W-10 | No response caching in widget | nova-chat.js |
| W-11 | Widget error messages too generic | nova-chat.js:1305 |
| W-12 | File attachment parsing catches bare Exception | app.py:22025 |
| W-13 | Enrichment keywords differ between chat/stream paths | app.py:22170 vs 22488 |

## Priority Fix Order
1. C-03: Deduplicate enrichment code
2. C-02: Parallelize API enrichment (ThreadPoolExecutor, 5-8s combined timeout)
3. C-01: Graceful fallback when all providers fail
4. C-05 + W-06: Increase history to 20 turns + add token counting
5. C-07: Incremental DOM updates instead of full innerHTML rewrite
6. W-01: True token streaming (providers support stream:true)
7. C-04 + C-08: Persistence reliability (retry + write-ahead)

## What's Working Well
- Rate limiting: robust, per-IP + global, thread-safe
- CSRF: double-submit cookie pattern correctly implemented
- API keys: not exposed client-side
- sessionStorage: 50-msg cap prevents memory leaks
- Error logging: consistent exc_info=True throughout
