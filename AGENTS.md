# AGENTS.md: rules for any coding agent working in this repo

This file is for agents other than Claude Code (OpenAI Codex and similar).
`CLAUDE.md` holds the same project rules for Claude sessions. Both are
authoritative. If you change a rule here, change it there too.

## 1. What this is

Nova AI Suite: a recruitment-marketing intelligence platform built by Shubham
Singh Chandel (Joveo). The core product is the **Media Plan Generator**: a
wizard takes a hiring brief (client, budget, duration, locations, roles,
industry, competitors) and returns a client-ready ZIP (branded PPTX deck +
XLSX workbook; PDF, HTML report, Sheets and email are separate exports).
Around it sit the Nova chatbot (RAG over
the `data/` knowledge base) and roughly 17 smaller product modules served
through the `/platform` shell and `/hub`.

- Live: https://media-plan-generator.onrender.com/
- Deploy: Render (Standard plan), **auto-deploys from `main` on every push**.
  A push to `main` is a production deploy.
- Health and deploy proof: `GET /api/deploy/ready` returns 200 after warm-up,
  with `git_commit`, `deploy_version`, an `embedding` block and a `retrieval`
  block. "Pushed" is not "deployed" until this endpoint reports your SHA.

## 2. Stack (do not change without an explicit owner decision)

- Python standard-library HTTP server in `app.py` (~25k lines). **No Flask,
  Django, FastAPI or any web framework.** Served by gunicorn with gevent
  workers via `wsgi.py`, `--preload` (see `render.yaml` startCommand).
- Production Python is **3.11** (`render.yaml` PYTHON_VERSION, CI uses 3.11).
  Some dev machines run 3.9. Code must work on both: on 3.9,
  `concurrent.futures.TimeoutError` is NOT the builtin `TimeoutError`, so
  import it explicitly (`from concurrent.futures import TimeoutError as
  _FutureTimeoutError`) whenever you catch a future/as_completed timeout.
- Templates: inline HTML/CSS/JS in `templates/` (the server inlines partials
  at STARTUP, so restart the server after editing a template). Static JS/CSS
  in `static/`, versioned with `?v=`.
- Knowledge base: JSON files in `data/`, loaded at startup.
- Services: Supabase (persistence, plan copies), Upstash Redis, Qdrant
  (vector store), Voyage AI embeddings (`voyage-4-lite`, 1024-d), an LLM
  router over many providers (`llm_router.py`), PostHog, Sentry, Slack.
- Document generation: `ppt_generator.py` (deck, via `deck_generator.py`),
  `excel_v2.py` (workbook), `pdf_generator.py`/`pdf_report.py` (PDF export),
  `bundle_qa.py` (the delivery gate that inspects every bundle before it
  ships).
- Plan generation lives in `app.py` (`/api/generate`, async job path). A
  second, synchronous copy of the pipeline exists in the same handler; if
  you change a stage, change both or consolidate them.

## 3. Commands

```bash
pip install -r requirements-dev.txt     # deps + test deps
python3 -m pytest tests/ -q             # full suite (~8,000 tests, a few min)
python3 app.py                          # local server (or ./start_dev.sh)
python3 -m black <file.py>              # formatter (pre-commit runs it)
```

- `tests/conftest.py` installs an **external-network guard**: any
  non-loopback socket connect fails instantly with a "network guard" error.
  Mock every external call. `NOVA_TESTS_ALLOW_NET=1` opts a whole run out;
  tests marked `live` are exempt per test.
- `scripts/deck_qa/` renders generated decks with real Poppins glyph metrics
  and reports text collisions. Run it for any deck layout change.
- CI (`.github/workflows/ci.yml`): compile, import check, offline
  plan-quality eval gate, pytest, prettier check.

## 4. Shipping

- Never force-push. Never push half-finished work to `main`: it deploys.
- Preferred: work on a feature branch and open a PR; the owner merges.
- If you have been told to ship directly, use
  `scripts/ship_from_worktree.sh` from a linked worktree on a feature branch
  with a clean tree. It rebases onto `origin/main`, runs the full suite,
  waits out a stability window, and fast-forward-pushes. Exit codes:
  0 shipped, 1 preflight, 2 rebase conflict (the script already aborted the
  rebase; rebase by hand, then rerun), 3 tests failed, 4 main too busy or
  push rejected (check the log for `GH013` = GitHub push protection found a
  secret-shaped string), 5 investigate.
- Do not edit a worktree while the ship script is running in it.
- Other agents (Claude sessions) push to `main` concurrently. Always
  `git fetch` and re-test on current `origin/main` before diagnosing a red
  test on main; someone may already have fixed it.
- Never commit runtime churn: `data/.embedding_cache.json`,
  `data/nova_memory_default.json`, `data/nova_response_cache.json`.
- Secret-shaped test literals (`phx_...`, `sk-...`) trip push protection.
  Build fake keys from fragments at runtime (`"ph" + "x_" + "FAKE"`).

## 5. Code standards

- Type hints on new functions. f-strings only (no `.format()`, no `%`).
- No bare `except:`; catch specific exceptions. Wrap every external API
  call in try/except with `logger.error(..., exc_info=True)`.
- NoneType safety: `data.get("key") or ""`, not `data.get("key", "")`.
  But beware precedence: `x.get(k) or 0 == 0` means `x.get(k) or True`.
  Parenthesise: `(x.get(k) or 0) == 0`. `tests/test_precedence_lint.py`
  enforces this.
- Error isolation: each data collector in its own try/except, so one
  failing source never kills a plan.
- Thread safety: locks around shared state. Workers are separate processes;
  module globals are NOT shared across workers (use the shared-state layer).
- Never cache an env-var key in a module global on first read; read the
  environment fresh each call (tests patch the environment).
- All animations respect `prefers-reduced-motion`.
- Brand colours come from `joveo_brand_2026.py`; never hard-code brand hexes.
  Fonts: Poppins (headings), Inter (body). Generated decks and the Nova chat
  use the light Joveo deck format.
- Behaviour change means a test change. Prove a timeout handler with a real,
  controlled timeout, not a mocked success path.

## 6. Invariants learned the hard way (do not regress these)

1. **Every client-visible number traces to a real source, or it is hidden.**
   Show "n/a" or omit the element. Never a placeholder, never a fabricated
   testimonial, logo, stat or benchmark.
2. **Currency is declared, never converted.** One resolver:
   `plan_currency.currency_for_plan_with_basis` (explicit field > symbol the
   client typed > markets if unanimous > USD). The deck, workbook, scorecard
   and `bundle_qa` must agree, or the gate fails correct bundles. Never
   compare a plan-currency figure with a USD benchmark (that is a hidden
   FX rate of 1.0): emit the neutral no-badge status. When resolving a
   market from a location, pass the whole `"City, ST"` string to
   `currency_for_country` (it holds the US-state guard so "Denver, CO" is
   not Colombia).
3. **One fact, one code path.** A word and a number describing the same
   thing (intensity label vs weight, preview budget vs server budget) must
   come from one function, or they drift. Budget parsing is shared by the
   wizard preview, `/api/estimate` and `/api/generate`.
4. **Mandatory disclosures live inside the element they qualify**, never
   behind a "draw only if it fits" guard, and never in a text frame passed to
   `_autofit_textframe` (it drops trailing paragraphs). Let optional text
   yield instead.
5. **KB JSON: metadata keys go LAST** in their object. Nova's deep search
   truncates matched objects to their first few keys, so a provenance note
   placed early evicts real data from what the model sees.
6. **Adding a data entry? Grep every reader of that file** for bare
   `[...]` indexing. A new seasonal entry without multipliers once crashed
   Nova chat.
7. **Test where users are.** Sales users run the wizard inside the
   `/platform` shell (`static/js/platform.js` injects `/fragment/<module>`),
   not only at `/media-plan`. Reproduce UI bugs there, including a tab switch.
8. **Location strings**: never split "City, ST" or "City, Country" on every
   comma; never fuzzy-correct a real place to a different town; a bare
   ambiguous name gets no borrowed metro (`geo_market.py` is the one
   resolver).
9. **`except TimeoutError`** around futures: see section 2.
10. **Deck verification**: a clean linter is not a pass. Render the deck and
    read it; confirm any visual suspicion against extracted text.
11. **CSS**: string tests are cascade-blind. Verify colours and visibility
    with `getComputedStyle` in a real render (watch for `!important`
    overrides from the `.studio` reskin).

## 7. Retrieval (Nova chat) facts

- Embeddings: Voyage `voyage-4-lite`, 1024-d, Qdrant collection
  `nova_knowledge__voyage-4-lite_1024` (~6.2K points). Collections are
  model- and dimension-scoped (blue/green), so a model change self-migrates
  into a new collection.
- Workers attach to Qdrant lazily on first search. `qdrant_attached: false`
  on a fresh, idle instance is expected. `indexed_documents: 0` is expected
  (the in-memory index is intentionally not built in prod). Health signals
  are `retrieval.can_retrieve` and `embedding.qdrant_point_count`.
- Voyage calls go through one cross-process rate-limit primitive
  (`vector_search._voyage_reserve_slot`), per-model windows, with a caller
  deadline so abandoned workers do not spend quota. Do not add a second,
  unthrottled Voyage client.
- `/api/chat` requires a signed-in @joveo.com user in production.

## 8. Where the history lives

- `git log` is the source of truth for what shipped. Commit messages are
  detailed on purpose.
- `docs/` holds audits, runbooks and design notes. `SESSION_HANDOFF.md` is
  stale (June 2026); verify anything in it against the code.
- Ask the owner before: deleting or migrating data, changing secrets, auth,
  billing, permissions or production config, sending anything external, or
  any irreversible architecture change.
