# NOVA_CHAT_GATE: widget-token plumbing for guest chat

Status: shipped OFF (2026-11-03). Turning it on changes production auth, which
is on the owner's pause list: flip it only with the owner's go-ahead.

## What this is, honestly

On its own the gate gives roughly **zero protection against scripted spend**.
The mint (`POST /api/chat/widget-token`) is admitted by the same Origin /
Referer check that admits a chat turn today, and that header is forgeable, so
a script needs exactly one extra request to get a token. Tokens are not a
leak defence either: every reply mints a fresh one, so whoever holds a live
token can keep a conversation going indefinitely, and anyone can mint a new
one. Cross-site spending through visitors' browsers was already blocked
before this change (the chat endpoints' JSON content-type check and the
WebSocket Origin allowlist).

What it is: **the plumbing for a real check at the mint**. Every guest turn
now has to present a server-issued, conversation-bound token that came from
one endpoint. Put a bot check (for example Cloudflare Turnstile) or a login
requirement on that endpoint and it governs all guest chat, with no further
change to the chat endpoints or the three clients. Until then, flipping the
flag buys almost nothing and costs one mint request per page session.

## The flip (one line)

Render dashboard, service `media-plan-generator`, Environment:

```
NOVA_CHAT_GATE=signed
```

Saving the variable redeploys. Roll back by deleting it (or setting `off`).
After the deploy, the boot log shows one line:
`Nova chat gate: NOVA_CHAT_GATE=signed (... signing secret: <source>; TTL 900s)`.

## Mechanics

1. The widget (`static/nova-chat.js`) and the `/nova` page
   (`static/js/nova.js`) call `POST /api/chat/widget-token` with
   `{"conversation_id": "<id>"}` before their first message, waiting at most
   3 s. The platform drawer (`static/js/platform.js`) mints lazily: it posts
   first and mints only on a `WIDGET_TOKEN_REQUIRED` refusal, then resends
   once, so under `off` it makes no extra request at all. Under `off` the mint answers
   `{"gate": "off"}` and the client stops asking. A failed mint (timeout,
   network error, 429) means "send without a token for 60 s".
2. The token is `v1.<conversation id>.<expiry>.<HMAC-SHA256>`, bound to one
   conversation id and valid 15 minutes (`TOKEN_TTL_SECONDS`). That TTL is
   also the idle expiry: a conversation idle for 15 minutes needs a new mint.
3. HTTP chat calls send it as `X-Nova-Widget-Token`; WebSocket chat messages
   carry it as `widget_token` (browsers cannot set headers on an upgrade).
4. Every authorised reply returns a fresh token for the conversation the
   turn actually used (the server may move a turn to a new conversation).
5. Rejection: HTTP 401 with code `WIDGET_TOKEN_REQUIRED`, or over the socket an
   error frame with that code. Clients re-mint and resend once, and only on
   that code; any other 401 is shown as it was before. Nothing paid runs
   before the check. A malformed token (non-ASCII or over-long expiry) is a
   401, never a 500.

Still admitted without a token under `signed`: the admin key (`X-Admin-Key`
or `Authorization: Bearer <ADMIN_API_KEY>`), `X-Nova-Api-Key` keys in
`NOVA_API_KEYS`, an HMAC-verified Supabase JWT, and the signed session cookie.
`/api/nova/chat` is admin-only already and unchanged.

Needs a token under `signed`: every header-only path (same-origin
Origin/Referer, the widget-domain substring fallback for `cg-automation` and
`geoviz`, and the legacy unsigned `nova_user_email` cookie accepted while
`STRICT_AUTH` is off).

A widget token only names a conversation. Reading or writing a stored
conversation still needs that conversation's own session token
(`nova_persistence.resolve_chat_conversation`), so a token never opens someone
else's conversation.

## Secret

Order: `NOVA_WIDGET_TOKEN_SECRET`, else derived from `SESSION_SIGNING_SECRET`
(domain-separated HMAC), else a boot-time random. The random is created when
`app.py` imports `nova_chat_gate`; under gunicorn `--preload` (the Procfile)
that happens once in the master, so both workers share it. It changes on
every deploy, which invalidates outstanding tokens; clients re-mint on the
401. Without `--preload`, set one of the env secrets.

## Blast radius of the flip

- Browser visitors on the three first-party clients: one extra POST per page
  session (widget and `/nova`; the drawer one extra round trip on its first
  turn under `signed`); otherwise no visible change. A page opened before the deploy runs
  the old client, whose first turn after the flip gets a 401 shown as a
  sign-in message; a reload fixes it (cache-busted `?v=`).
- Embedded clients on other Joveo hosts that rely on the widget-domain Origin
  fallback (no API key) stop working until they send `X-Nova-Api-Key`.
  `static/nova-embed.js` with `apiKey` set is unaffected.
- Scripts and monitors calling `/api/chat` without a credential: already 401
  today without an allowed Origin (for example the "Smoke test - Nova chat"
  step in `.github/workflows/deploy-verify.yml` sends neither); with only a
  forged Origin they now need the extra mint call. Give them the admin key.
- Rollback: delete the variable; no data or schema involved.

## Under `off` (default)

Every chat endpoint behaves as before: no token is read and none is returned.
The clients ask the mint once per page session and then send nothing extra.
An unknown value (for example `NOVA_CHAT_GATE=sign`) keeps the gate off and
logs one WARNING, so a typo cannot lock every visitor out.

## Next step that would make it real

Add the bot check or login at the mint: verify a Turnstile token (or require
a signed session) in `routes/chat.py::handle_widget_token_post` before
minting. Mind the 3 s client timeout: an invisible Turnstile widget must
resolve before the first send, or the client sends without a token and gets
one 401 retry.

## Code and tests

- `nova_chat_gate.py` (mint, check, mode, secret), `app.py`
  (`_chat_gate_needs_token`, `/api/chat`, `/api/chat/stream`, `/ws/chat`,
  CSRF exemption), `routes/chat.py::handle_widget_token_post`, `wsgi.py`
  (boot line).
- `tests/test_nova_chat_gate.py`: both modes over live HTTP and a raw
  WebSocket, expiry, wrong binding, forged and malformed tokens, spoofed
  Origin, unsigned cookie and widget-domain fallback, signed cookie /
  API key / admin key admitted, a successful turn returning a fresh token.
- `tests/test_nova_widget_chat_gate_runtime.py` (widget under node: all
  transports, plain 401 not retried, hanging mint, 429 back-off),
  `tests/test_nova_client_conversation_token.py` (`/nova` page and drawer),
  `tests/test_nova_chat_ux.py` (one silent re-mint).
