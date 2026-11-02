# NOVA_CHAT_GATE: signed widget tokens for guest chat

Status: shipped OFF (2026-11-03). Turning it on changes production auth, which
is on the owner's pause list: flip it only with the owner's go-ahead.

## The flip (one line)

Render dashboard, service `media-plan-generator`, Environment:

```
NOVA_CHAT_GATE=signed
```

Saving the variable redeploys. Roll back by deleting it (or setting `off`);
that redeploys too. Optional: set `NOVA_WIDGET_TOKEN_SECRET` to a long random
value first if the app is ever run without gunicorn `--preload` (see Secret).
After the deploy, the boot log shows one line:
`Nova chat gate: NOVA_CHAT_GATE=signed (... signing secret: <source>; TTL 900s)`.

## What it does

Today a "same-origin guest" reaches `/api/chat`, `/api/chat/stream` and
`/ws/chat` on the strength of its `Origin` / `Referer` header, which any
non-browser caller can set, so a script can spend paid LLM and Voyage budget
with one header. Under `signed`, a guest turn also needs a widget token:

1. The chat client (widget `static/nova-chat.js`, the `/nova` page
   `static/js/nova.js`, the platform drawer `static/js/platform.js`) calls
   `POST /api/chat/widget-token` with `{"conversation_id": "<id>"}` before its
   first message. The mint is admitted on today's chat terms (real credential
   or same-origin page) and sits under the per-IP limit for `/api` POSTs.
2. The token is `v1.<conversation id>.<expiry>.<HMAC-SHA256>`: bound to that
   one conversation id, valid 15 minutes (`TOKEN_TTL_SECONDS`).
3. HTTP chat calls send it as `X-Nova-Widget-Token`; WebSocket chat messages
   carry it as `widget_token` (browsers cannot set headers on an upgrade).
4. Every authorised reply returns a fresh token for the conversation the turn
   actually used (the server may move a turn to a new conversation), so an
   active chat never re-mints. A 401 makes the client re-mint and resend once.

Rejections: missing, malformed, forged, expired, or bound to another
conversation -> HTTP 401 with code `WIDGET_TOKEN_REQUIRED`, or over the socket
an error frame `{"type": "error", "code": "WIDGET_TOKEN_REQUIRED", "error":
"HTTP 401: ...", "done": true}`. Nothing paid runs before the check.

Who is NOT affected under `signed`: the admin key (`X-Admin-Key` or
`Authorization: Bearer <ADMIN_API_KEY>`), `X-Nova-Api-Key` keys in
`NOVA_API_KEYS`, an HMAC-verified Supabase JWT, and the signed session cookie.
`/api/nova/chat` is admin-only already and unchanged.

Who now needs a token under `signed`: every header-only path: same-origin
Origin/Referer, the widget-domain substring fallback (`cg-automation`,
`geoviz`), and the legacy unsigned `nova_user_email` cookie (accepted while
`STRICT_AUTH` is off; anyone can type it, so it must not bypass the gate).

Replay: a widget token only names a conversation. Reading or writing a stored
conversation still needs that conversation's own session token
(`nova_persistence.resolve_chat_conversation`), so a replayed widget token
never opens someone else's conversation; at most it starts fresh ones until
it expires, under the existing per-IP chat limit (15/min).

## Secret

Order: `NOVA_WIDGET_TOKEN_SECRET`, else derived from `SESSION_SIGNING_SECRET`
(domain-separated HMAC, so a widget token can never pass as a cookie
signature), else a boot-time random. The random is created when `app.py`
imports `nova_chat_gate`; under gunicorn `--preload` (the Procfile) that
happens once in the master, so both workers share it. It changes on every
deploy, which invalidates outstanding tokens; clients re-mint on the 401.
Without `--preload`, set one of the env secrets or tokens will fail on
whichever worker did not mint them.

## Blast radius of the flip

- Browser visitors on the three first-party clients: one extra POST before
  the first message of a page session; otherwise no visible change. A page
  opened before the deploy has the old client; its first turn after the flip
  gets a 401 and the old client shows "sign in" instead of retrying. Clients
  are cache-busted (`nova-chat.js?v=`, `nova.js?v=`), so a reload fixes it.
- Embedded clients on other Joveo hosts that rely on the widget-domain Origin
  fallback (no API key) stop working until they send `X-Nova-Api-Key`.
  `static/nova-embed.js` with `apiKey` set is unaffected.
- Scripts and monitors calling `/api/chat` without a credential: already 401
  today without an allowed Origin (for example the "Smoke test - Nova chat"
  step in `.github/workflows/deploy-verify.yml` sends no Origin and no key);
  with only a spoofed Origin they now 401 as well. Give them the admin key.
- Rollback cost: delete the variable; no data or schema involved.

## What it does not do

It is not user authentication. A scripted client can still fetch a token
from the mint first, exactly like a browser. What the gate buys: no paid turn
on a forged header alone; one choke point (the mint) where a bot check or a
login requirement can be added later; tokens that leak through logs expire
in 15 minutes and open one conversation; a page on another site cannot read
a minted token (no CORS grant), so it cannot spend through visitors' browsers.

## Under `off` (default)

Every chat endpoint behaves exactly as before: no token is read, none is
returned, the WebSocket and HTTP gates are untouched. The mint answers
`{"gate": "off"}`; the clients ask it once per page session and then send
nothing extra. An unknown value (for example `NOVA_CHAT_GATE=sign`) keeps the
gate off and logs one WARNING, so a typo cannot lock every visitor out.

## Code and tests

- `nova_chat_gate.py` (mint, check, mode, secret), `app.py`
  (`_chat_gate_needs_token`, `/api/chat`, `/api/chat/stream`, `/ws/chat`,
  CSRF exemption), `routes/chat.py::handle_widget_token_post`, `wsgi.py`
  (boot line).
- `tests/test_nova_chat_gate.py`: both modes over live HTTP and a raw
  WebSocket, expiry, wrong binding, spoofed Origin, unsigned cookie and
  widget-domain fallback, admin key untouched, mint rules.
- `tests/test_nova_widget_chat_gate_runtime.py` (widget under node, all
  transports), `tests/test_nova_client_conversation_token.py` (`/nova` page
  and drawer helpers), `tests/test_nova_chat_ux.py` (one silent re-mint).
