"""Permanent regression corpus for the plan-figure guard (nova_plan_figures.enforce).

The guard exists to stop Nova quoting hires / applications / CPA / cost-per-hire
figures that are not the planning engine's. Its first version also turned TRUE
figures into confident WRONG ones: a user's own goal ("to make 50 hires"), a
third party's rate ("agencies charge $4,500 per hire", "6 hires per month", "57
applications per hire"), one channel's funnel ("Indeed: $12,000 at a $29 CPA
yields about 414 applications"), a follow-up comparison ("Compared with 17-35
hires at $60k, $120k gives ...") and fragments of percentages ("12%" read as
"1") were all rewritten into the whole plan's range. An independent verifier
collected those cases (the first two blocks below, ``v1_`` and ``v2_``); the
rest (``own_``) widen them: salaries, BLS counts, job-board counts, user echoes,
comparisons, per-channel CPCs, non-US currencies, tables, code blocks, URLs,
percentages, ranges in words.

GOVERNING RULE the expectations encode: MARK-ONLY, never rewrite. A plan figure that
traces to no tool result of the turn and to no number the user typed gets a
dagger after it and nothing else changes -- the reported 900 / 14 / $66.67 and the
recommender's 917 / $65.43 included, whatever the sentence's shape. (Until
2026-10-18 an allowlist of "plan-subject" shapes was REWRITTEN to the engine's
range; three adversarial rounds each found true or conditional figures rewritten,
so those cases now expect the mark; tables are mark-only too, see
``test_nova_plan_guard_corpus3``.) See
``test_nova_plan_guard_corpus2`` for the second corpus. Everything that is not a
plan claim at all (a ratio, code, a URL, a percentage, a year, a question, a
statement about an agency or a competitor) is left byte-for-byte alone.

Every case carries its input answer, the user message, the tool results of the
turn (frozen JSON captured from the real projection tool, so the corpus does not
drift with the engine) and the exact expected body. ``expected`` is the answer
text before the guard's appendix (the basis line), which is checked separately.
"""

from __future__ import annotations

import json
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import pytest

for _name in (
    "anthropic",
    "openai",
    "supabase",
    "redis",
    "qdrant_client",
    "sentence_transformers",
    "posthog",
):
    sys.modules.setdefault(_name, types.ModuleType(_name))

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import nova  # noqa: E402
import nova_plan_figures as pf  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures" / "nova_plan_guard"
MSG = "make a $60k, 3-month media plan for customer service reps in Chicago"

# The turn's tool results. p60 / p120 are frozen output of the real projection
# tool for a customer-service brief in Chicago: 2,400-7,500 applications,
# 17-35 hires, $8-$25 per application, $1,675-$3,350 per hire at $60k.
TOOLS = {
    "p60": json.loads((FIXTURES / "projection_60k_generic.json").read_text("utf-8")),
    "p120": json.loads((FIXTURES / "projection_120k_generic.json").read_text("utf-8")),
    "salary": {
        "role": "Customer Service Representative",
        "median_salary": 41000,
        "percentile_25": 35000,
        "source": "BLS",
    },
    "bench": {
        "industry": "general",
        "benchmarks": {
            "appcast_median_cpa": 35,
            "median_cpc": 1.2,
            "apply_rate": "5.19%",
        },
        "summary": "Appcast median CPA is $35",
    },
    "cph_table": {
        "industry_cph": {
            "healthcare": {"cost_per_hire": 9000},
            "retail": {"cost_per_hire": 2700},
        }
    },
    "meta": {"estimated_applies": 1200, "estimated_cpa": 50, "source": "Meta Ads"},
    # a second calculator's plan totals that slipped past the withholding
    "recommender": {
        "recommendation": "Programmatic first, then regional boards.",
        "total_projected": {
            "applications": 917,
            "hires": 17,
            "cost_per_application": 65.43,
        },
    },
}


@dataclass(frozen=True)
class Case:
    id: str
    text: str
    message: str
    tools: Tuple[str, ...]
    kind: str  # "unchanged" | "marked" | "rewritten"
    expected: Optional[str] = None
    plan_context: bool = True


CASES = [
    Case(
        "v1_salary",
        "The median salary for a customer service rep in Chicago is $41,000 (BLS).",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60", "salary"),
        "unchanged",
    ),
    Case(
        "v1_salary85",
        "Expect an $85,000 median salary for senior reps.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_bls_openings",
        "BLS projects 2.9M openings for customer service reps through 2033.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_postings",
        "There are 12,500 postings on Indeed for this role in Chicago right now.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_apply_rate",
        "The typical apply rate is 5.19%, versus 3.22% on LinkedIn.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60", "bench"),
        "unchanged",
    ),
    Case(
        "v1_date",
        "Launch on March 3, 2027 and run through June 2027.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_appcast_grounded",
        "A CPA of $35 is the Appcast median for this category.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60", "bench"),
        "unchanged",
    ),
    Case(
        "v1_appcast_cpa_noncue",
        "Indeed's CPA runs about $35 for this role.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60", "bench"),
        "unchanged",
    ),
    Case(
        "v1_user_budget",
        "With your $60k budget, here is how I'd split it.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_user_goal_hire",
        "To hire 14 nurses, start with niche boards.",
        "I need to hire 14 nurses with $60k over 3 months",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_user_goal_14hires",
        "Your target of 14 hires is within reach.",
        "plan $60k over 3 months, we want 14 hires",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_user_brief_for50",
        "To make 50 hires you will need a larger budget.",
        "make a media plan for 50 software engineers in Austin with $60k",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_user_brief_goal_colon",
        "Your goal of 14 hires looks ambitious at this budget.",
        "Hiring goal: 14 RNs. Budget $60k for 3 months.",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_user_brief_openings",
        "For your 25 openings, a 3-month plan is reasonable; that is 25 hires total.",
        "$60k plan, 25 openings for CSRs in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_market_size",
        "The Chicago CSR talent pool is about 48,000 workers.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_competitor_cpc",
        "Competitors bid about $1.20 per click on Indeed.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_channel_cpc",
        "Programmatic CPC averages $0.85 and LinkedIn $3.10.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_competitor_hires",
        "Acme Corp made 1,200 hires in Chicago last year.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_hires_per_month",
        "That works out to roughly 6 hires per month.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_recruiter_load",
        "A recruiter typically handles 15 hires per quarter.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_agency_fee",
        "Staffing agencies charge about $4,500 per hire for these roles.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_agency_fee_bench",
        "Staffing agencies typically charge $4,500 per hire.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_cph_bench_shrm",
        "SHRM puts average cost per hire at $5,475.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_cph_nurses_noncue",
        "Cost per hire for registered nurses is around $9,000.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_engine_range_ok",
        "Expect 2,400–7,500 applications and 17–35 hires at $8–$25 per application.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_engine_k_fmt",
        "Cost per hire of $1.7k-$3.4k on the engine basis.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_engine_K_to",
        "Cost per hire of $1.7K to $3.4K.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_engine_tilde",
        "Roughly ~2,400 applications at the low end.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_engine_words",
        "between 2.4k and 7.5k applications",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_euro",
        "In Germany expect a CPA of €18 and a cost per hire of €2,900.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_rupee",
        "In India the CPA is ₹350 per application.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_yen",
        "Japan CPA averages ¥2,800 per application.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_url",
        "See https://example.com/report?hires=14&apps=900 for the data.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_codeblock",
        "```python\nhires = 14\napplications = 900\n```",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_pct_apps",
        "Applications: 52% came from mobile.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_pct_hires",
        "Hires are 12% of qualified applicants on average.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_apps_per_opening",
        "Most employers see 3 applications per opening for this role.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_mobile_apps",
        "Candidates use 3 apps like Indeed and LinkedIn to search.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_meta_placements",
        "Meta offers 12 placements across Facebook and Instagram.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_year_hires",
        "Q3 2026 hires in retail slowed nationally.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_two_plans",
        "At $60k you get 17–35 hires; at $120k you get 35-70 hires.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="At $60k you get 17–35 hires; at $120k you get 35-70† hires.",
    ),
    Case(
        "v1_prior_campaign",
        "Last campaign you got 900 applications from Indeed alone.",
        "we got 900 applications last campaign; plan $60k for 3 months",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_ratio",
        "Plan on about 57 applications per hire.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_one_hire_budget",
        "One hire takes about $1,675-$3,350 on this basis.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_time_to_hire",
        "Time to hire averages 44 days; expect first hires within 30 days.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_apply_starts",
        "Apply starts grew 20% YoY.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_cpa_target_user",
        "Your target CPA of $20 is inside the engine range.",
        "plan $60k over 3 months with a target CPA of $20",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_cpa_under_user",
        "Keeping cost per application under $15 is aggressive.",
        "60k over 3 months, keep cost per application under $15",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_table_tool_cph",
        "| Industry | Cost per hire |\n|---|---|\n| Healthcare | $9,000 |\n| Retail | $2,700 |",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60", "cph_table"),
        "unchanged",
    ),
    Case(
        "v1_table_salary",
        "| Role | Median salary | Hires needed |\n|---|---|---|\n| CSR | $41,000 | 14 |",
        "need 14 CSRs, $60k over 3 months",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_hire_count_question",
        "No, 900 is the number of postings, not hires.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_headcount",
        "Your team of 40 recruiters can support this.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_cpc_range",
        "CPCs range $0.50-$2.50 depending on channel.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_budget_split",
        "Put $21,721 (36.2%) into programmatic and $12,000 into Indeed.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_ratio_conv",
        "At a 2% click-to-apply rate, 10,000 clicks yield 200 applications.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_hires_last_year_client",
        "You made 300 hires last year, so this plan covers about a quarter of that.",
        "we made 300 hires last year; plan $60k for 3 months",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_placements_staffing",
        "Staffing firms report 85 placements per recruiter annually.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_cpa_meta_tool",
        "Meta CPA in Chicago is around $22.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_benchmark_word_apps",
        "Typically you'd get 900 applications for $60k.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Typically you'd get 900† applications for $60k.",
    ),
    Case(
        "v1_invented_no_cue",
        "Expect 900 applications, 14 hires and a CPA of $66.67.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Expect 900† applications, 14† hires and a CPA of $66.67†.",
    ),
    Case(
        "v1_comp_tool_recommender",
        "Expect ~917 applications and 17 hires at $65.43 avg CPA.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60", "recommender"),
        "marked",
        expected="Expect ~917† applications and 17 hires at $65.43† avg CPA.",
    ),
    Case(
        "v1_percent_cph",
        "Cost per hire is about 4.4% of salary, so $1,804 for a $41k role.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v1_hire_rate",
        "Hire rate: 2 hires per 100 applications is normal.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v2_words_range",
        "Expect between 2,400 and 7,500 applications and between 17 and 35 hires.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v2_words_cpa",
        "Cost per application falls between $8 and $25 per application.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v2_channel_funnel",
        "Programmatic: $21,000 at a $1.50 CPC buys about 14,000 clicks; at a 5% apply rate that is about 700 applications.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v2_channel_indeed",
        "Indeed: $12,000 at a $29 CPA yields about 414 applications.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Indeed: $12,000 at a $29† CPA yields about 414† applications.",
    ),
    Case(
        "v2_followup_compare",
        "Compared with 17–35 hires at $60k, $120k gives 35–70 hires.",
        "what if I double it to $120k?",
        ("p120",),
        "marked",
        expected="Compared with 17–35† hires at $60k, $120k gives 35–70† hires.",
    ),
    Case(
        "v2_followup_compare2",
        "Going from $60k to $120k moves hires from 17–35 to 35–71.",
        "what if I double it to $120k?",
        ("p120",),
        "unchanged",
    ),
    Case(
        "v2_nonplan_q_with_proj",
        "Nurse CPA nationally is about $45 per application.",
        "what's the CPA for nurses?",
        ("p60",),
        "unchanged",
    ),
    Case(
        "v2_no_tool_plan_ctx",
        "With $60k you can expect 900 applications and 14 hires at a CPA of $66.67.",
        "I have $60k for 3 months for CSRs in Chicago, what can I expect?",
        (),
        "marked",
        expected="With $60k you can expect 900† applications and 14† hires at a CPA of $66.67†.",
    ),
    Case(
        "v2_no_tool_nonplan",
        "Expect around 900 applications and 14 hires.",
        "what can a small campaign get?",
        (),
        "unchanged",
        plan_context=False,
    ),
    Case(
        "v2_cph_question_no_tool",
        "Cost per hire for nurses is typically $9,000-$12,000.",
        "what's the cost per hire for nurses?",
        (),
        "unchanged",
    ),
    Case(
        "own_salary_range",
        "Registered nurses in Chicago earn $72,000 to $98,000 a year.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_bls_employment",
        "BLS counts 3.1 million customer service representatives nationally.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_job_board_count",
        "Joveo's network includes 1,238 publishers; 17 job boards serve Chicago.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_user_echo_hires",
        "40 hires over three months is a stretch at this budget.",
        "I need 40 hires for our call center on $60k",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_user_echo_apps",
        "5,000 applications is inside the engine's range.",
        "we want 5,000 applications from $60k",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_foreign_budget_hires",
        "At $90k you would see about 26 hires.",
        "what about $90k?",
        ("p60",),
        "marked",
        expected="At $90k you would see about 26† hires.",
    ),
    Case(
        "own_two_budgets_second_bad",
        "At $60k you get 17–35 hires; at $30k, roughly 9–17 hires.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="At $60k you get 17–35 hires; at $30k, roughly 9–17† hires.",
    ),
    Case(
        "own_channel_cpc_prose",
        "Indeed CPCs run $1.10–$1.80; LinkedIn $3.40.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_channel_hires_ok",
        "Programmatic alone models 8 hires.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_channel_hires_bad",
        "Programmatic alone models 40 hires.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Programmatic alone models 40† hires.",
    ),
    Case(
        "own_gbp",
        "In the UK expect £14 per application and £2,400 per hire.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_cad_prefix",
        "Toronto runs CA$22 per application.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_euro_hire",
        "Germany costs €2,900 per hire.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_table_salary_only",
        "| Metric | Value |\n|---|---|\n| Median salary | $41,000 |",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_table_hires_user",
        "| Role | Hires needed |\n|---|---|\n| CSR | 14 |",
        "need 14 CSRs, $60k over 3 months",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_table_hires_wrong",
        "| Channel | Projected Hires |\n|---|---|\n| Programmatic | 99 |",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="| Channel | Projected Hires |\n|---|---|\n| Programmatic | 99† |",
    ),
    Case(
        "own_code_py",
        "```\nhires = 14\n```",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_inline_code",
        "Set `hires: 14` in the config.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_url_query",
        "Open /plan?hires=14&applications=900 to see it.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_md_link",
        "[report](https://x.io/a?apps=900&hires=14)",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_pct_hires_ref",
        "17% of hires come from referrals.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_pct_apps_conv",
        "About 5% of applications convert to interviews.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_words_from_to_ok",
        "Expect from 17 to 35 hires.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_words_between_bad",
        "Expect between 20 and 50 hires from this plan.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Expect between 20 and 50† hires from this plan.",
    ),
    Case(
        "own_year_hires",
        "2026 hires are tracking ahead of 2025.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_year_range",
        "Between 2025 and 2027 hires will grow.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_per_opening",
        "Expect about 40 applications per opening.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_per_week",
        "That is 3 hires a week.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_for_every",
        "For every 100 applications you get 2 hires.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_hypothetical",
        "If you budget $60k you could hire 14 nurses.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_question_to_user",
        "Do you want 14 hires or 20 hires?",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_goal_cue",
        "To hit 14 hires you would need more budget.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="To hit 14† hires you would need more budget.",
    ),
    Case(
        "own_want_cue",
        "If you want 50 hires, raise the budget.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="If you want 50† hires, raise the budget.",
    ),
    Case(
        "own_last_plan",
        "The last plan delivered 22 hires.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_best_case_single",
        "Expect 17–35 hires; the best case of 35 hires needs everything to go right.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_up_to",
        "You could see up to 35 hires.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_bold_rewrite",
        "Expect **14 hires** and **900 applications** from this plan.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Expect **14† hires** and **900† applications** from this plan.",
    ),
    Case(
        "own_list_rewrite",
        "1. Expect 14 hires from this plan.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="1. Expect 14† hires from this plan.",
    ),
    Case(
        "own_decimal_neighbour",
        "With $60,000 you get roughly 900 applications at about $66.67 each.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="With $60,000 you get roughly 900† applications at about $66.67 each.",
    ),
    Case(
        "own_k_unsupported",
        "Expect 3k applications from this plan.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Expect 3k† applications from this plan.",
    ),
    Case(
        "own_cpas_plural",
        "Good nurse CPAs run $20 to $60.",
        "what is a good CPA for nurses?",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_monthly_budget_text",
        "Allocate $12,000 per month to programmatic.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_pay_per_hire",
        "Indeed costs $90 per hire on a pay-per-hire plan.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Indeed costs $90† per hire on a pay-per-hire plan.",
    ),
    Case(
        "own_meta_tool_count",
        "Meta alone would bring 1,200 applications.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60", "meta"),
        "unchanged",
    ),
    Case(
        "own_meta_tool_count_plan",
        "Expect 1,200 applications from this plan.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60", "meta"),
        "marked",
        expected="Expect 1,200† applications from this plan.",
    ),
    Case(
        "own_dagger_present",
        "Expect 14† hires from this plan.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_sentence_decimal_then_claim",
        "Your CPA of $66.67 pairs with a cost per hire of $4,286.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Your CPA of $66.67† pairs with a cost per hire of $4,286†.",
    ),
    Case(
        "own_competitor_named",
        "Acme made 5,000 hires in Chicago last year.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_linkedin_stat",
        "LinkedIn reports 20 hires per 1,000 applications.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_hires_colon_ok",
        "Hires: 17–35 (an estimate).",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_hires_colon_bad",
        "Hires: 60.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Hires: 60†.",
    ),
    Case(
        "own_mobile_apps_word",
        "Most candidates use 3 apps to search.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_apply_starts_bad",
        "Expect 9,000 apply starts from this plan.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Expect 9,000† apply starts from this plan.",
    ),
    Case(
        "own_no_claims",
        "Programmatic and global boards carry the plan.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_ordinal",
        "Your 3rd hire arrives in week 6.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own_range_dash_en_bad",
        "Expect 20–50 hires and 1,000–2,000 applications from this plan.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Expect 20–50† hires and 1,000–2,000† applications from this plan.",
    ),
    Case(
        "own2_channel_hires_from",
        "Expect 8 hires from the programmatic channel.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own2_ambiguous_that_means",
        "That means roughly 14 hires.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="That means roughly 14† hires.",
    ),
    Case(
        "own2_per_month_apps",
        "Plan on about 900 applications per month.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own2_plan_total_budget_ref",
        "Your 60k budget buys 900 applications.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Your 60k budget buys 900† applications.",
    ),
    Case(
        "own2_channels_count",
        "The plan includes 3 channels and 14 hires.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="The plan includes 3 channels and 14† hires.",
    ),
    Case(
        "own2_first_month",
        "Expect 14 hires in the first month.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Expect 14† hires in the first month.",
    ),
    Case(
        "own2_or_more",
        "Expect 14 hires or more.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Expect 14† hires or more.",
    ),
    Case(
        "own2_cpa_supported_single",
        "A CPA of $8 is aggressive.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own2_conditional_cpa",
        "Your CPA could be $45 if you target nurses.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Your CPA could be $45† if you target nurses.",
    ),
    Case(
        "own2_versus_channels",
        "Programmatic beats LinkedIn: 14 hires vs 3.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own2_one_in_three",
        "Roughly 1 in 3 applications converts to an interview.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own2_no_more_than",
        "No more than 14 hires are expected.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="No more than 14† hires are expected.",
    ),
    Case(
        "own2_cpa_cpa_paren",
        "Cost per application (CPA) for nurses in Texas is $55.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own2_typical_cpa_range",
        "Typical CPA for nurses: $35-$85.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own2_wk_scope",
        "By week 6 you will have 14 hires.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="By week 6 you will have 14† hires.",
    ),
    Case(
        "own2_mixed_good_bad",
        "Expect 17–35 hires, and 900 applications overall.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Expect 17–35 hires, and 900† applications overall.",
    ),
    Case(
        "own2_plan_total_good",
        "Expect 2,400–7,500 applications and 17–35 hires from this plan.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own2_q_scope",
        "In Q1 you should see 14 hires.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="In Q1 you should see 14† hires.",
    ),
    Case(
        "own2_table_apps_ok",
        "| Channel | Applications |\n|---|---|\n| Programmatic | 2,961 |\n| Niche | 128 |",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own2_table_apps_bad",
        "| Channel | Applications |\n|---|---|\n| Programmatic | 9,999 |",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="| Channel | Applications |\n|---|---|\n| Programmatic | 9,999† |",
    ),
    Case(
        "own2_heading_numbers",
        "### Plan 14 hires?\nNo.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own2_cph_noncue_cap",
        "Your cost per hire of $9,000 is high.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Your cost per hire of $9,000† is high.",
    ),
    Case(
        "own2_cph_under_user",
        "Your cost per hire target of $2,000 sits inside the range.",
        "plan $60k, target cost per hire of $2,000",
        ("p60",),
        "unchanged",
    ),
    Case(
        "own2_comma_big_apps",
        "Expect 12,000 applications from this plan.",
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        ("p60",),
        "marked",
        expected="Expect 12,000† applications from this plan.",
    ),
]

assert len(CASES) >= 100 and len({c.id for c in CASES}) == len(CASES)

_APPENDIX_STARTS = ("\n\n*Projection basis:",)


def _raws(case: Case):
    return [json.dumps(TOOLS[name]) for name in case.tools]


def _run(case: Case, text: Optional[str] = None):
    return pf.enforce(
        case.text if text is None else text,
        _raws(case),
        case.message,
        plan_context=case.plan_context,
        plan_shaped=False,
    )


def _split(out: str):
    cuts = [out.find(m) for m in _APPENDIX_STARTS if out.find(m) >= 0]
    cut = min(cuts) if cuts else len(out)
    return out[:cut], out[cut:]


def _fig():
    engines, _ = pf.collect([json.dumps(TOOLS["p60"])])
    return engines[0]


@pytest.mark.parametrize("case", CASES, ids=[c.id for c in CASES])
def test_corpus_case(case):
    out, stats = _run(case)
    body, tail = _split(out)
    if case.kind == "unchanged":
        assert (
            body == case.text
        ), f"{case.id}: an answer that makes no plan claim was altered"
        assert stats["rewritten"] == 0 and stats["marked"] == 0
    elif case.kind == "marked":
        assert body == case.expected
        assert stats["rewritten"] == 0, f"{case.id}: marked cases are never rewritten"
        assert stats["marked"] >= 1 and pf.DAGGER in body
    else:
        raise AssertionError(f"{case.id}: no case may expect a rewrite")
    # the only thing the guard may add after the answer is its basis line
    for piece in [p for p in tail.split("\n\n") if p.strip()]:
        assert piece.startswith("*Projection basis:")


@pytest.mark.parametrize("case", CASES, ids=[c.id for c in CASES])
def test_corpus_case_is_idempotent(case):
    once, _ = _run(case)
    twice, stats = _run(case, once)
    assert twice == once
    assert stats["rewritten"] == 0


def test_no_case_is_ever_rewritten():
    """The guard is mark-only: no case of this corpus is rewritten, whatever
    its expectation."""
    assert not [c.id for c in CASES if c.kind == "rewritten"]
    for case in CASES:
        _out, stats = _run(case)
        assert stats["rewritten"] == 0, case.id


def test_the_reported_and_the_recommender_figures_are_marked():
    by_id = {c.id: c for c in CASES}
    out, _ = _run(by_id["v1_invented_no_cue"])
    assert (
        _split(out)[0]
        == "Expect 900\u2020 applications, 14\u2020 hires and a CPA of $66.67\u2020."
    )
    # the plan as the sentence's subject changes nothing: still only marked
    out, stats = pf.enforce(
        "This plan delivers 900 applications, 14 hires and a CPA of $66.67.",
        [json.dumps(TOOLS["p60"])],
        MSG,
        plan_context=True,
    )
    assert (
        _split(out)[0]
        == "This plan delivers 900\u2020 applications, 14\u2020 hires and a CPA of $66.67\u2020."
    )
    assert stats["rewritten"] == 0 and "corrected" not in out
    # a rival calculator's own projected totals are not a market benchmark
    out, _ = _run(by_id["v1_comp_tool_recommender"])
    assert "917\u2020" in out and "$65.43\u2020" in out
    # "about 900 applications" with a benchmark word is marked, not rewritten
    out, _ = _run(by_id["v1_benchmark_word_apps"])
    assert _split(out)[0] == "Typically you'd get 900\u2020 applications for $60k."


def test_the_basis_line_goes_only_with_figures_the_engine_stands_behind():
    """Quoting the plan's own range earns the basis line; echoing the user's goal,
    a third party's rate or code earns nothing."""
    by_id = {c.id: c for c in CASES}
    quoted, _ = _run(by_id["v1_engine_range_ok"])
    assert _split(quoted)[1].startswith(pf._basis_line(_fig()).join(["\n\n", ""])[:20])
    for cid in (
        "v1_user_goal_14hires",
        "v1_user_brief_for50",
        "v1_agency_fee",
        "v1_url",
        "v1_codeblock",
    ):
        out, _ = _run(by_id[cid])
        assert out == by_id[cid].text, cid


# ---------------------------------------------------------------------------
# Through Nova: the same cases, plus the marker, the cache and the routes
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def engine():
    return nova.Nova()


@pytest.fixture(autouse=True)
def _flags(monkeypatch):
    monkeypatch.setenv("NOVA_NUMBER_VERIFIER_ENABLED", "1")
    monkeypatch.setenv("NOVA_CITATIONS_ENABLED", "0")
    monkeypatch.setenv("NOVA_PLAN_FIGURE_GUARD_ENABLED", "1")


def _t3(engine_, text, message, tools, query_type=None):
    nova._set_turn_tool_capture(
        {"conversation_id": "", "tools": {}, "tools_used": [], "stateless": True}
    )
    try:
        result = {
            "response": text,
            "sources": [],
            "confidence": 0.8,
            "tools_used": [],
            "_tool_results_raw": [json.dumps(TOOLS[t]) for t in tools],
        }
        if query_type:
            result["query_type"] = query_type
        return engine_._apply_t3_postprocessing(result, message)
    finally:
        nova._set_turn_tool_capture(None)


def test_nova_pipeline_marks_the_reported_answer_and_leaves_the_rest(engine):
    reported = (
        "For $60,000 over three months Nova projects 900 applications, 14 hires and "
        "a CPA of $66.67, a cost per hire of $4,286."
    )
    out = _t3(engine, reported, MSG, ("p60",))["response"]
    for marked in (
        "900\u2020 applications",
        "14\u2020 hires",
        "$66.67\u2020",
        "$4,286\u2020",
    ):
        assert marked in out
    assert (
        "17\u201335 hires" not in out
        and "corrected to Nova's planning engine" not in out
    )
    assert out.count(nova._T3_UNVERIFIED_FOOTER) == 1
    for cid in (
        "v1_agency_fee",
        "v1_hires_per_month",
        "v1_ratio",
        "v1_pct_hires",
        "v1_competitor_hires",
    ):
        case = next(c for c in CASES if c.id == cid)
        out = _t3(engine, case.text, case.message, case.tools)["response"]
        assert "17\u201335" not in out and "2,400\u20137,500" not in out, cid


def test_the_guarded_flag_makes_a_second_pass_a_no_op(engine):
    first = _t3(engine, "Expect 14 hires from this plan.", MSG, ("p60",))
    assert first["plan_guarded"] is True and "14\u2020 hires" in first["response"]
    again = engine._apply_t3_postprocessing(dict(first), MSG)
    assert again["response"] == first["response"]


def test_nothing_invisible_is_added_to_the_reply(engine):
    out = _t3(engine, "Programmatic and global boards carry the plan.", MSG, ("p60",))
    assert out["response"] == "Programmatic and global boards carry the plan."
    out = _t3(
        engine,
        "Your goal of 14 hires looks ambitious at this budget.",
        "goal: 14 hires, $60k budget",
        ("p60",),
    )
    assert out["response"] == "Your goal of 14 hires looks ambitious at this budget."


# ---- the cache ------------------------------------------------------------
def test_cache_keys_are_versioned_past_the_unguarded_entries():
    import nova_cache

    # v5 retired the keys that dropped the decimal point ($1.5k == $15k)
    assert nova._CACHE_VERSION == "v5"
    assert nova._normalize_cache_key("what hires for 60k").startswith("v5:")
    assert nova_cache._CACHE_PREFIX.startswith("nova_chat:v5:")


STALE = "For $60,000 over three months Nova projects 900 applications, 14 hires and a CPA of $66.67."


def test_a_stale_cache_hit_is_checked_on_read(engine):
    """An entry written before the guard (no ``plan_guarded``) replays the
    model's raw figures; with no tool results to rewrite from, they are marked."""
    hit = engine._guard_cache_hit({"response": STALE, "sources": []}, MSG)
    assert (
        "900\u2020 applications" in hit["response"]
        and "14\u2020 hires" in hit["response"]
    )
    assert nova._T3_UNVERIFIED_FOOTER in hit["response"]
    assert hit["plan_guarded"] is True


def test_a_guarded_cache_hit_is_not_touched_again(engine):
    guarded = _t3(
        engine, "Expect between 17 and 35 hires from this plan.", MSG, ("p60",)
    )
    hit = engine._guard_cache_hit(dict(guarded), MSG)
    assert hit["response"] == guarded["response"]
    assert pf.DAGGER not in hit["response"]


def test_a_cache_hit_with_no_plan_claim_is_returned_as_is(engine):
    entry = {"response": "Nurses are in short supply.", "sources": []}
    out = engine._guard_cache_hit(dict(entry), "how hard is it to hire nurses")
    assert out["response"] == entry["response"]


def test_chat_serves_a_cache_hit_through_the_guard(engine, monkeypatch, tmp_path):
    monkeypatch.setattr(nova, "_response_cache", {})
    monkeypatch.setattr(nova, "RESPONSE_CACHE_FILE", tmp_path / "resp.json")
    monkeypatch.setattr(nova, "_upstash_enabled", False)
    monkeypatch.setattr(nova, "_intelligent_cache_get", lambda *a, **k: None)
    monkeypatch.setattr(nova, "_intelligent_cache_set", lambda *a, **k: False)
    message = (
        "what can I expect from a $60k budget over three months? corpus-cache-check"
    )
    nova._set_response_cache(
        nova._normalize_cache_key(message),
        {"response": STALE, "sources": [], "confidence": 0.9, "tools_used": []},
    )
    out = engine.chat(message, [], None)
    assert "900\u2020 applications" in out["response"]
    assert "<!--" not in out["response"]  # no internal marker reaches the reader


# ---- the degraded / rule-based route --------------------------------------
def test_the_rule_based_budget_route_records_the_engine_result(engine):
    """All models down: the route calls the projection directly. Its own figures
    (ranges, per-channel hires, 'projected 0 hires' alerts) must count as traced,
    not be marked '† not traced'."""
    message = "What can a $60k budget deliver for 10 nurses in Texas over 3 months?"
    nova._set_turn_tool_capture(
        {"conversation_id": "", "tools": {}, "tools_used": [], "stateless": True}
    )
    try:
        result = engine._chat_rule_based(message, None, [])
        assert "query_budget_projection" in nova._get_turn_tool_capture()["tools"]
        text = engine._apply_t3_postprocessing(dict(result), message)["response"]
    finally:
        nova._set_turn_tool_capture(None)
    assert pf.DAGGER not in text, text


# ---- the block is for plans, not for one-line answers ------------------------
def test_a_one_line_answer_in_a_plan_chat_gets_no_projected_outcomes_block(engine):
    out = _t3(
        engine,
        "The median salary for a customer service rep in Chicago is $41,000.",
        MSG,
        ("p60", "salary"),
        query_type="media_plan",
    )["response"]
    assert "Projected outcomes" not in out
