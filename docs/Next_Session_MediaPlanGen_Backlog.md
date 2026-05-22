# Media Plan Generator — Next Session Backlog

**Date**: 2026-05-22
**Last touched in this session**: NOT directly touched (Nova chatbot session). G4 production-validator agent independently verified **NO REGRESSION** from chatbot changes.

## ✅ Current state verified by G4 (read this first)

The S76-S79b Nova refactor did NOT break the media plan generator:
- `/api/generate` endpoint still functional
- Plan templates (joveo_slides_template.py, deck_generator.py) untouched
- No `from nova import` statements in any plan-gen support module
- Live smoke tests passed:
  - `GET /api/health/ping` → 200 in 0.43s
  - `GET /` → 200 in 0.59s (135KB)
  - `OPTIONS /api/generate` → 204 with CORS
  - `POST /api/generate` (no auth) → 401 AUTH_REQUIRED (gate working)
  - `POST /api/generate` (with Origin) → began plan generation, no 500/NameError
- Shared JSONs (`joveo_publishers.json`, `joveo_global_supply_repository.json`) untouched

Full report: `docs/MediaPlan_Regression_Audit_2026-05-22.md`

## 🟡 What COULD benefit from a dedicated session

The chatbot session did NOT audit the media plan generator end-to-end. The following are based on memory of recent sessions (S46-S49) — verify before acting.

### Pre-existing backlog from S49

Per MEMORY.md S49:
> Pass rate 10% → 84%. Resource util 60% → 95%. 5 plan audits. plan_validator.py (6 checks). 80+ salary roles. CPC v4. 45-country salary. 44 KB files. 12 API enrichment tasks. Supabase 48→300 salary rows. Data Sources slide removed. All branding "Created by Shubham Singh Chandel". Chatbot 110s→45s.

What was likely deferred:
- Reach the last 16% (84% → 100% pass rate). Look at `plan_validator.py` for the 6 checks; see which are failing on actual plans.
- Resource utilization: 95% is "very full" — heading toward limits. Consider Render plan upgrade or memory trim.
- The "Regenerate old plans" pending item: WeLocalize + Benchmark + CareFirst on new build to verify 84%+

### Recent data inputs the chatbot session created (now available to plan gen)

These data files are now in the repo and could benefit plan generation:
- `data/intl_role_benchmarks_v1.json` (505KB, 996 cited cells, 5 verticals × 15 countries)
- `data/industry_reports_2026.json` (109KB, 80 verified reports with key metrics)
- `data/ta_leaders_curated_2026.json` (108KB, 89 TA-leader citations)

**Potential wins** for plan-gen:
- Use F1's role-by-country data to localize the auto-generated plans for non-US clients
- Use F2's industry report key numbers to enrich the "Industry Context" slide with cited 2026 stats
- Use F3's TA-leader content for the "Trends to Watch" slide with attributed quotes

### Things to AUDIT before changing anything

1. **Plan validator pass rate** — re-run `python3 plan_validator.py` against a known-good plan; what's the current pass rate?
2. **Resource utilization** — `curl https://media-plan-generator.onrender.com/api/health` returns resource metrics
3. **Last successful plan generation** — check Slack #nova-media-plans channel for recent plan URLs
4. **CPC v4 calibration** — is the v4 model still aligned with 2026 benchmarks? Compare against new `intl_role_benchmarks_v1.json` GBR/IND/DEU numbers
5. **Joveo branding consistency** — last audit was S46; verify "Created by Shubham Singh Chandel" footer on all slides
6. **80+ salary roles** — verify coverage is still complete; expand if F1's data adds new roles
7. **Russian/Cyrillic support** — F1 expanded currency to RUB/UAH; does plan gen render Cyrillic?

### Quality gates to verify before next deploy

- `python3 plan_validator.py <path-to-test-plan.json>` — should pass all 6 checks
- POST `/api/generate` with a known-good payload — should return ZIP in <60s
- Slack notification fires to `#nova-media-plans`
- PPT renders cleanly in PowerPoint AND Google Slides
- Excel opens cleanly in Excel AND Numbers AND Google Sheets

### Suggested first questions for the next session

1. "What's the current `/api/generate` pass rate on a sample of 10 recent plans?"
2. "Has anyone shipped a non-US plan (UK/India/Germany) and verified currency rendering?"
3. "Is the chatbot's new `_query_kb_deep` (28 datasets) being used to inform plans?"
4. "Are F1/F2/F3 datasets reachable from plan_generator?"

### Operational pre-flight

When opening a fresh media plan generator session:
1. Read MEMORY.md S49 entry (latest plan-gen-specific work)
2. Read `docs/MediaPlan_Regression_Audit_2026-05-22.md` (proves no regression from chatbot work)
3. Read this file
4. `git log --oneline -10 -- "*.py" "*template*"` to see recent plan-gen-relevant changes
5. Don't touch nova.py or chatbot tests — that's a separate product
6. Plan-gen test suite locations: `tests/test_deck_generator.py`, `tests/test_templates.py`

## 🚨 Cross-product considerations

If you change shared symbols, the chatbot may break:
- `_COUNTRY_ALIASES`, `_COUNTRY_CURRENCY`, `_get_currency_for_country` — used by both products
- `joveo_publishers.json`, `joveo_global_supply_repository.json` — both products read these
- The 5 newly-wired stranded healthcare files — chatbot expects specific schema

Before any cross-product change, run BOTH test suites:
```bash
# Chatbot
python3 -m pytest tests/test_country_awareness.py tests/test_intl_chatbot_scenarios.py tests/test_currency_formatting.py tests/test_us_regression_safety.py -q

# Plan gen
python3 -m pytest tests/test_deck_generator.py tests/test_templates.py tests/test_e2e.py -q
```

Both should be green before pushing.
