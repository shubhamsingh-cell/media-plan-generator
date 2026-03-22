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
- Colors: PORT_GORE=#202058, BLUE_VIOLET=#5A54BD, DOWNY_TEAL=#6BB3CD
- Design: Vercel/Linear-inspired dark theme, glassmorphism, aurora gradients
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
