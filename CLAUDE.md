# Nova AI Suite

## 1. Project Overview
AI-powered recruitment intelligence platform with 17 products for media planning, competitive intel, performance tracking, and talent acquisition. Built by Shubham Singh Chandel.

## 2. Tech Stack
- **Server**: Python stdlib HTTP server (NO Flask/Django)
- **Templates**: Inline HTML in `templates/` (22 files), served directly
- **Fonts**: Google Fonts (Inter)
- **Deploy**: Render.com (paid Standard tier), auto-deploy from `main`
- **URL**: https://media-plan-generator.onrender.com/

## 3. Architecture
- `app.py` -- Main server, routes, all API endpoints
- `templates/` -- HTML templates with inline CSS/JS
- `data/` -- Knowledge base loaded at startup
- `static/` -- Chat widget JS, admin panel
- Entry point: `hub.html` (product suite) and `index.html` (media plan generator)

## 4. Coding Standards
- See `.claude/rules/python.md` for Python rules
- See `.claude/rules/nova-project.md` for project-specific rules
- Key patterns: type hints, `or ""` for NoneType safety, error isolation, f-strings

## 5. Key Patterns
- **Error isolation**: Each data collector in its own try/except
- **Thread safety**: Use locks for shared state
- **NoneType safety**: `data.get("key") or ""` not `data.get("key", "")`
- **API calls**: Always wrap in try/except with `logger.error(exc_info=True)`

## 6. Workflows
- `/deploy` -- Pre-flight checks, push, verify
- `/qc` -- Quality control on uncommitted changes
- `/research` -- RPI feasibility analysis before implementation
- `/tdd` -- Test-driven development workflow
- `/save-state` -- Save session state for continuity
- `/create-skill` -- Meta-skill to generate new skills
- `/audit-config` -- Audit configuration against best practices

## 7. Brand
- Canonical palette (Joveo deck 2026): see `joveo_brand_2026.py` — INDIGO=#202058, PURPLE=#5A54BE, TEAL=#6BB5CE, MAGENTA=#B7669E, lavender surfaces #F4F4FF/#ECEAF7, canvas #FFFCF9. Do NOT hardcode brand hexes; import from `joveo_brand_2026.py`.
- Fonts: Poppins (headings), Inter (body).
- Generated presentations (Slides/PPTX/PDF) + Nova chatbot follow the deck's clean LIGHT format. Legacy product dashboards remain the Vercel/Linear dark theme (glassmorphism, aurora gradients) by design.
- LinkedIn: https://www.linkedin.com/in/chandel13/

## 8. Quality Gates
- Always run QC agent after code changes
- Never skip the simplify pass
- All animations must respect `prefers-reduced-motion`
- No bare `except:` -- catch specific exceptions

## 9. Important Files
- `.claude/rules/` -- Coding rules (Python, project-specific)
- `.claude/commands/` -- Slash commands (deploy, qc, tdd, etc.)
- `.claude/agents/` -- Custom agents (code-reviewer, qc-agent)
- `.claude/skills/` -- Domain skills (qc, deploy patterns)
- `rpi/.template/` -- RPI workflow templates

## 10. Do Not
- Do NOT use Flask, Django, or any web framework
- Do NOT commit API keys or secrets to git
- Do NOT skip QC checks before deploying
- Do NOT use `.format()` or `%` string formatting
- Do NOT use bare `except:` clauses

## 11. Shipping with concurrent sessions
Multiple Claude sessions can push to `main` at the same time, and Render
auto-deploys from `main` on every push. Never push directly from a
worktree by hand -- use `scripts/ship_from_worktree.sh` from a linked
worktree on a feature branch; it rebases, runs the full suite, waits out
a stability window, and only fast-forward-pushes (never force).

## 12. Conclude every session: decide, ship, deploy (owner standing directive, 2026-09-24)
The owner wants every session in this repo to finish its own work end to end
without waiting for a go. This is durable, pre-given authorization for the
normal ship path: commit, `scripts/ship_from_worktree.sh` (push to `main`),
and the Render auto-deploy that follows.

- **Decide, don't ask.** Make routine product and engineering calls yourself
  and state each decision and its reason in the final report. Pause for the
  owner ONLY on the global pause list: deleting or migrating data, secrets,
  billing, auth or permissions, production config, external communications,
  or an irreversible architecture choice.
- **Done means live.** A session is concluded only when (1) nothing it changed
  is left uncommitted, (2) its commits are on `origin/main` (check with
  `git cherry origin/main <branch>`: all `-` or empty), and (3)
  `/api/deploy/ready` reports that SHA or a later one. Quote the served SHA
  in the final report. "Pushed" is not "deployed".
- **Quality gates still run first.** Full suite green; deck and UI changes
  rendered and pixel-checked before shipping (see `scripts/deck_qa/`). A
  failing gate means fix it and ship, not stop and ask.
- **Other sessions' work.** If another session's branch is committed but its
  session has been idle 30+ minutes, ship it via the script from that branch.
  If its session is still active, leave it. Never commit another session's
  UNcommitted changes blind: message that session to conclude instead.
- **Never ship runtime churn.** `data/.embedding_cache.json`,
  `data/nova_memory_default.json` and `data/nova_response_cache.json` are
  rewritten by tests and servers; restore or ignore them, never commit them.
- **Stale branches.** A branch whose `git cherry` is all `-` against
  `origin/main` is fully superseded: tag it `archive/<name>` and delete it.
- **Enforced at stop.** A user-level Stop hook
  (`~/.claude/hooks/mpg-conclude-guard.py`) blocks ending a turn while this
  worktree has uncommitted changes or commits not on `origin/main`. It stays
  quiet while a ship run or pytest is running here. If you are genuinely
  blocked on the pause list above, or the owner said not to ship, end the
  reply with one line starting `CONCLUDE-PAUSE:` that names the reason.
  Never use that line to skip shipping finished work.
