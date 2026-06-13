# Agentic Media-Plan Generation — Design

**Status:** Proposed (design only — no pipeline rewrite in this change)
**Owner:** infra_docs
**Last updated:** 2026-06-13
**Related tasks:** L3 "Agentic media-plan generation (design first)", L3 "Eval gating in CI"
**Related code:** `data_synthesizer.synthesize`, `budget_engine.calculate_budget_allocation`, `plan_validator.validate_plan`, `supabase_data.get_real_outcomes`, `trend_engine.get_benchmark`, `llm_router.call_llm_json`, `eval_framework.EvalSuite`, `scripts/eval_gate.py`

---

## 0. TL;DR

Today a media plan is produced by a **fixed, linear pipeline**:

```
enrich  ->  synthesize  ->  budget  ->  validate
```

Every plan walks the same four steps in the same order, regardless of how
unusual the request is. This is predictable, cheap, and easy to reason
about — and it should stay the default.

This document proposes adding an **optional agentic loop** in which an LLM
*orchestrates* the same underlying engines as **tools** (the budget engine,
benchmark lookups, validators, and the real-outcomes warehouse), deciding
which to call, in what order, and when to stop. It is strictly **opt-in
behind a feature flag**, **gated by the existing eval harness**
(`eval_framework` + `scripts/eval_gate.py`), and **instantly reversible**
(flip the flag off → byte-for-byte the current behavior).

No engine is rewritten. The agent is a thin orchestrator that calls the
*same* functions the linear pipeline already calls; the deterministic path
remains the source of truth and the fallback.

---

## 1. Motivation

### 1.1 What the fixed pipeline does well

- **Determinism.** Same inputs → same plan. Critical for client trust and
  for the visual-regression / baseline tests.
- **Cost & latency.** No extra LLM round-trips for orchestration; the LLM is
  used only where it already is (narratives, structured extraction).
- **Auditability.** Every number traces to a known step.

We are **not** trying to replace any of this.

### 1.2 Where the fixed order is too rigid

The linear order bakes in assumptions that don't always hold:

1. **No re-planning.** If `validate_plan` flags that projected CPA exceeds
   the client's stated budget ceiling (`_check_cpa_vs_budget`), the pipeline
   can only auto-correct mechanically (rescale dollars). It cannot decide
   *"this role mix is unaffordable at this budget — drop the niche boards and
   re-run the allocation with a cheaper channel set,"* which is exactly what
   a human planner would do.

2. **Benchmarks fetched once, blindly.** Enrichment pulls benchmarks up
   front. If `get_real_outcomes(title, location)` returns a high-confidence
   measured outcome for *some* roles but a no-match for others, the pipeline
   treats them uniformly. An agent could notice the gap and go fetch a
   sibling-role or sibling-geo benchmark to fill it.

3. **One-shot synthesis.** `data_synthesizer.synthesize` runs once. There is
   no "the validator complained, let me revise just that section and
   re-validate" step short of a full re-run.

4. **Hard to express planner intent.** "Maximize hires under a fixed budget
   but never let any single channel exceed 40%" is a constrained-optimization
   loop. Today that's only expressible as code changes in `budget_engine`.

An agentic loop turns these from *pipeline rewrites* into *runtime decisions*
the model can make per-request, while every number it produces still comes
from the same trusted engines.

### 1.3 Why now / why gated

We just landed a **CI eval gate** (`scripts/eval_gate.py`) on top of
`eval_framework.EvalSuite` (budget sanity, collar consistency, geographic
coherence, CPA reasonableness — 120+ cases). That gate is the precondition
that makes an agentic mode *safe to try*: we can measure whether the loop
produces plans at least as good as the linear pipeline before exposing it to
any client, and the gate blocks a regression from shipping.

---

## 2. Design principles (non-negotiable)

1. **Additive & opt-in.** Flag default OFF. The linear pipeline is untouched
   and remains the default and the fallback. (HARD RULE 4.)
2. **Tools wrap existing engines.** The agent calls the *same* functions the
   pipeline calls. No business logic moves into the agent. No engine
   re-implements anything.
3. **The agent never fabricates numbers.** All figures originate from a tool
   return value. The model orchestrates and explains; it does not invent CPA,
   salaries, or hire counts. (HARD RULE 5.)
4. **Deterministic fallback always available.** Any agent failure (budget
   exhausted, tool error, invalid output, timeout) → fall back to the linear
   pipeline result. The user always gets a plan.
5. **Gated by evals.** The agentic path must clear `scripts/eval_gate.py`
   (same harness as the linear path, plus agent-specific criteria in §6)
   before it can be enabled for a cohort.
6. **One-switch rollback.** Disabling is a flag flip, not a deploy.

---

## 3. Tool surface

The agent is given a small, typed catalog of tools, each a thin wrapper over
an existing function. Wrappers add: (a) JSON-serializable I/O, (b) argument
validation, (c) a per-call audit record, (d) a `provenance` tag on every
returned figure. **No wrapper introduces new numeric logic.**

| Tool name              | Wraps                                              | Purpose | Read/Write |
|------------------------|---------------------------------------------------|---------|------------|
| `get_real_outcomes`    | `supabase_data.get_real_outcomes(title, location)`| Real Joveo `cg_benchmarks` outcomes (avg_cost, cost_per_apply, sample_size, confidence). Graceful no-match. | read |
| `get_benchmark`        | `trend_engine.get_benchmark(...)`                 | Industry/role/geo CPA & demand benchmarks. | read |
| `allocate_budget`      | `budget_engine.calculate_budget_allocation(...)`  | The core engine: channel split, clicks/applies/hires projections, ROI rebalance. | read (pure) |
| `validate_plan`        | `plan_validator.validate_plan(data)`              | Cross-checks (salary↔role, CPA↔budget, allocation sum, hires consistency, location sanity). Returns findings + severities. | read (computes) |
| `synthesize_section`   | `data_synthesizer` section helpers + `llm_router.call_llm_json` | Regenerate one narrative/section against a schema, not the whole plan. | read |
| `propose_channel_mix`  | `channel_recommender` (existing)                  | Suggest a channel-percentage map for a collar/industry/budget. | read |
| `finalize_plan`        | (terminal) assembles the working draft into the canonical plan dict | The agent's only way to "return" — output must pass `validate_plan` with no `error`-severity findings. | terminal |

**Tool contract rules**

- Every tool takes and returns **plain JSON** (dict/list/scalar). Wrappers
  marshal to/from the engines' native signatures (e.g. `allocate_budget`
  maps the JSON args onto `calculate_budget_allocation`'s keyword args:
  `total_budget`, `roles`, `locations`, `industry`, `channel_percentages`,
  `collar_type`).
- Every figure returned carries a `provenance` field
  (`"cg_benchmarks"`, `"trend_engine"`, `"budget_engine"`, …) so the agent —
  and the deliverable — can cite sources and never present a fabricated
  number as measured.
- All wrappers are **side-effect-free** except `finalize_plan`. They never
  write to Supabase, never mutate `data/audit_log.jsonl`, never call out to
  paid APIs that the linear pipeline wouldn't.
- Tool calls are issued through the existing structured-LLM primitive
  `llm_router.call_llm_json` (provider-agnostic, validated, 1 retry) so the
  agent inherits router fallback, cost accounting, and JSON validation.

---

## 4. Control flow

### 4.1 Where it plugs in

The agentic loop is an **alternative implementation of the same entry point**
that produces the plan dict consumed by the Excel/PPT/PDF generators. It
sits behind a dispatcher:

```
generate_plan(request):
    if feature_flags.agentic_enabled(request):   # cohort / flag check
        try:
            plan = run_agentic_loop(request)      # §4.2
            if plan is not None:
                return plan
        except Exception:
            log_and_count("agentic_fallback")
    return run_linear_pipeline(request)           # enrich->synthesize->budget->validate
```

The linear pipeline is the fallback for *every* failure mode. The dispatcher
is the **only** new branch on the critical path, and it is a no-op when the
flag is off.

### 4.2 The loop

```
run_agentic_loop(request):
    state = seed_state(request)            # roles, locations, budget, constraints
    budget = AgentBudget(max_steps=N, max_tokens=T, max_wall_s=W)   # see §5

    while not budget.exhausted():
        decision = llm_router.call_llm_json(
            messages = build_messages(state, tool_catalog),
            schema   = TOOL_CALL_OR_FINALIZE_SCHEMA,
            system_prompt = AGENT_SYSTEM_PROMPT,   # principles in §2 restated
        )
        if not decision.ok:
            return None                    # -> deterministic fallback

        if decision.data.action == "finalize":
            draft = decision.data.plan
            findings = validate_plan(draft)            # mandatory gate
            if has_error_severity(findings):
                state.append_validator_feedback(findings)
                continue                                # one more revise loop
            return attach_metadata(draft, state.audit)  # success

        # action == "call_tool"
        result = dispatch_tool(decision.data.tool, decision.data.args)
        state.record(decision, result)     # audit trail + provenance

    return None                            # budget exhausted -> fallback
```

Key properties:

- **`validate_plan` is mandatory** before any agent output is accepted. The
  agent cannot ship a plan the linear pipeline would have rejected.
- **Bounded revision.** A failing validation feeds findings back exactly
  once per finalize attempt; the step budget caps total iterations.
- **Every output is a real plan dict** in the canonical shape, so downstream
  generators (`excel_v2`, `ppt_generator`, `pdf_generator`) are unchanged.

### 4.3 Typical trajectory (illustrative)

1. `get_real_outcomes("RN - ICU", "TX")` → measured, high confidence.
2. `get_real_outcomes("Phlebotomist", "TX")` → no-match.
3. `get_benchmark(...)` to fill the phlebotomist gap → industry CPA.
4. `propose_channel_mix(collar="blue", industry="healthcare", budget=...)`.
5. `allocate_budget(...)` with that mix.
6. `validate_plan(draft)` → finding: CPA vs budget over ceiling.
7. `allocate_budget(...)` again with a leaner mix (agent's decision).
8. `validate_plan(draft)` → clean.
9. `finalize_plan(draft)`.

The linear pipeline could only do steps 1, 5, 6 (mechanical rescale), 9.

---

## 5. Guardrails

| Guardrail | Mechanism |
|-----------|-----------|
| **Step budget** | Hard cap `max_steps` (e.g. 12). On exhaustion → fallback. Prevents infinite tool loops. |
| **Token / cost budget** | Per-request `max_tokens` and a $ ceiling tracked via `llm_router` cost accounting. Over budget → fallback. |
| **Wall-clock budget** | `max_wall_s` (e.g. 30s for plans, tighter for Q&A). Over → fallback. |
| **No fabrication** | Every numeric field must map to a tool-returned figure with `provenance`. `finalize_plan` rejects any number lacking provenance. |
| **Mandatory validation** | `validate_plan` must pass (no `error` severity) before output is accepted — identical bar to the linear path. |
| **Tool allowlist** | The agent can only call the catalog in §3. Unknown tool name → step rejected, counted, no execution. |
| **Read-only tools** | Only `finalize_plan` is terminal/assembling; nothing the agent calls writes to Supabase or `data/audit_log.jsonl`. (HARD RULE 2.) |
| **Deterministic fallback** | Any failure (LLM error, invalid JSON, budget, validation can't be satisfied) → linear pipeline. User always gets a plan. |
| **Prompt-injection containment** | Tool *outputs* (e.g. scraped JD text upstream) are passed as data, never as instructions; the system prompt states tool results are untrusted content. Reuses the red-team posture in `evals/redteam.yaml`. |
| **Full audit trail** | Every decision + tool call + result + provenance recorded in the plan's metadata for post-hoc review (not in the shared audit log). |
| **Idempotent tools** | All read tools are pure, so retries/re-runs are safe. |

---

## 6. Eval criteria (the gate to enable)

The agentic path must clear **two bars** before it is enabled for any cohort,
both enforced by `scripts/eval_gate.py`.

### 6.1 Parity on the existing harness

Run `eval_framework.EvalSuite` against plans produced by the **agentic
generator** (a new eval mode / fixture that routes generation through the
loop). The gate's floors and regression checks apply unchanged:

- Overall pass-rate ≥ the linear baseline (no regression beyond
  `max_regression`, default 3 pts).
- Every category (Budget Sanity, Collar Consistency, Geographic Coherence,
  CPA Reasonableness) clears its floor.

The gate already supports this: capture a linear-path baseline with
`python3 scripts/eval_gate.py --update-baseline`, then run the agentic-path
suite and let `--baseline` flag any regression.

### 6.2 Agent-specific criteria (new eval categories)

Add to `eval_framework` (separate task) and fold into the gate:

| Criterion | Measure | Bar |
|-----------|---------|-----|
| **Validation pass on first finalize** | % of agentic runs whose first `finalize` clears `validate_plan` | ≥ linear path's clean-rate |
| **Fallback rate** | % of requests that fall back to linear | ≤ 10% (tunable) — high fallback ⇒ the loop isn't earning its cost |
| **No-fabrication** | % of output figures with valid `provenance` | 100% (any miss = hard fail) |
| **Cost overhead** | median extra $ / plan vs linear | within an agreed budget (e.g. ≤ 2×) |
| **Latency overhead** | p95 wall-clock vs linear | within the wall-clock guardrail |
| **Quality lift (optional)** | LLM-rubric or human spot-check that the agentic plan is *better* on hard cases | net-positive on the curated hard-case set |

Optionally fold the Promptfoo LLM suite (`evals/promptfoo.yaml`,
`evals/redteam.yaml`) into the gate via `--promptfoo-results` so the agent's
prompt/guardrails are red-teamed before rollout.

---

## 7. Phased rollout

**Phase 0 — Eval gate in CI (done in this change).**
`scripts/eval_gate.py` runs `EvalSuite` and fails the build on regression.
This is the safety net every later phase depends on. No agent yet.

**Phase 1 — Tool wrappers + harness, flag OFF.**
Implement the §3 wrappers (pure, audited, provenance-tagged) and a
`run_agentic_loop` behind `agentic_enabled` (default OFF). Add the
agentic-path eval mode (§6). Nothing user-facing changes. Land the new eval
categories.

**Phase 2 — Shadow mode (internal only).**
For internal/dev requests, run *both* paths, serve the linear result, and log
a diff (numbers, validation findings, cost, latency). Build confidence that
agentic ≥ linear on real traffic without exposing it. Tune step/cost budgets
and the system prompt against the diffs.

**Phase 3 — Canary cohort.**
Enable `agentic_enabled` for a small allowlist (internal users, then 1–2
friendly clients). Gate continuously on §6 criteria. Watch fallback rate and
cost dashboards. Any gate regression → auto-disable (Phase rollback).

**Phase 4 — Progressive ramp.**
Expand cohort by percentage. Keep linear as default for the long tail of
unusual requests until parity is proven there too.

**Phase 5 — Default-on (only if earned).**
Flip default to ON *only* if the agentic path has held parity-or-better on
the gate across a sustained window, with linear retained as the permanent
fallback. This may never happen — and that's acceptable; the value is in the
hard cases, not in replacing the cheap default.

---

## 8. Rollback

Rollback is **a flag flip**, not a deploy:

- **Instant disable:** set `agentic_enabled` → OFF (env var / config /
  cohort list). Next request uses the linear pipeline. No code change, no
  deploy, no data migration.
- **Automatic disable:** if the CI gate regresses, or runtime dashboards
  breach a guardrail (fallback rate spike, cost spike, validation-failure
  spike), an alert (reuse `alert_manager` / `slack_alerter`) trips and the
  flag is auto-set OFF for the affected cohort.
- **Per-request safety:** even with the flag ON, any single failing request
  silently falls back to linear, so a partial outage never blocks plan
  delivery.
- **No state to unwind:** the agent's tools are read-only and the linear path
  is never modified, so disabling leaves the system byte-for-byte where it
  was. Audit trails from agentic runs remain in plan metadata for review.

---

## 9. Risks & mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| **Non-determinism erodes client trust** | High | High | Flag OFF by default; deterministic linear path stays the default. Agentic runs record full audit trail + provenance. Optionally pin temperature low / seed where the provider supports it. |
| **Cost/latency blow-up from tool loops** | Med | Med | Step/token/wall-clock budgets (§5); fallback on exhaustion; cost-overhead eval bar (§6.2); dashboards + auto-disable (§8). |
| **Agent fabricates numbers** | Low (by design) | Critical | Provenance required on every figure; `finalize_plan` rejects unsourced numbers; `validate_plan` mandatory; no-fabrication eval = 100% or hard fail. |
| **Prompt injection via upstream tool data** | Med | High | Tool outputs treated as untrusted data, never instructions; red-team suite (`evals/redteam.yaml`) folded into the gate; tool allowlist. |
| **Silent quality regression** | Med | High | This is exactly what the eval gate exists to stop — parity + regression checks block enablement and block CI. |
| **Maintenance burden of two paths** | Med | Med | Tools are *thin wrappers* — no duplicated business logic. Engines have one implementation; the agent just calls them. |
| **Agent gets stuck / high fallback rate** | Med | Low (user still served) | Fallback-rate eval bar (≤10%); if breached the loop isn't earning its keep → keep flag off and iterate on prompt/budgets. |
| **Provider/model drift changes agent behavior** | Med | Med | `llm_router` is provider-agnostic; the eval gate re-runs on model changes and blocks regressions — the same reason the gate was built. |

---

## 10. Explicitly out of scope (this document)

- Any change to `data_synthesizer`, `budget_engine`, `plan_validator`,
  `trend_engine`, or `supabase_data` — the engines are reused as-is.
- Implementing the tool wrappers or the loop (Phase 1+).
- New Supabase tables or writes.
- Changing the default generation path.

This is a **design**. The only code that ships alongside it is the CI eval
gate (`scripts/eval_gate.py`) that makes the later phases safe to attempt.

---

## Appendix A — Linear pipeline reference (today)

For grounding, the current stages and their entry points:

- **enrich** — benchmark/outcome lookups feed the data dict
  (`supabase_data.get_real_outcomes`, `trend_engine.get_benchmark`,
  channel/collar enrichment).
- **synthesize** — `data_synthesizer.synthesize(...)` builds the plan dict;
  narratives via `llm_router` / `call_llm_json`.
- **budget** — `budget_engine.calculate_budget_allocation(total_budget,
  roles, locations, industry, channel_percentages, collar_type)` produces the
  channel split + clicks/applies/hires projections + ROI rebalance.
- **validate** — `plan_validator.validate_plan(data)` runs cross-checks
  (salary↔role, demand↔temperature, CPA↔budget, allocation-sum,
  confidence-consistency, hires-consistency, location-sanity), auto-corrects
  where safe, and attaches `data["_validation"]`.

The agentic loop reuses **every one** of these via the tool surface in §3.
