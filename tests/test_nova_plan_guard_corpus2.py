"""Second permanent corpus for the plan-figure guard (nova_plan_figures.enforce).

The guard is a NET THAT MARKS. Its first three versions rewrote true figures into
confident wrong ones, so it now only marks:

* a hires / applications / CPA / cost-per-hire figure that traces to nothing gets
  a dagger (a mark); NOTHING is ever rewritten, in prose or in a table's Total
  row (the last Total-row rewrite was removed on 2026-10-18);
* a number the user typed in ANY turn of the conversation (digits or words) is
  theirs and is never touched;
* markdown tables are read by column header AND by the row's first cell
  ("| Hires | 14 |"), so a Metric | Value table behaves like the same facts in a
  bullet list.

An asymmetric rule runs through every expectation below: a dagger on a true
figure is noise, a rewrite of a true figure is harm. ``W`` (a wrongful rewrite)
must be zero.

Two blocks of cases are pinned:

* ``VERIFIER_CASES`` -- 131 cases an independent verifier wrote against the
  previous version, each with the expectation the verifier recorded BEFORE running
  it (the multi-turn ones carry the earlier user turn as ``turns``);
* ``OWN_CASES`` -- 90+ cases in categories the first corpus did not cover:
  earlier-turn briefs, role-based counts and staff / team phrasing, ranges with
  units, tables with totals rows in both layouts, numbered lists and headings,
  bold labels, quoted ad copy, markdown links, code, and non-English formats.
  Their expectations were written before the guard ran on them.

Expectation codes (the verifier's):

* ``U``   untouched: no rewrite and no dagger;
* ``OK``  engine-correct, or a user echo: no rewrite and no dagger;
* ``NR``  never rewritten: a dagger is acceptable, any other change is not;
* ``R``   a wrong plan total: it must be MARKED (until 2026-10-18 it was
  rewritten, in prose and, for ``own_t1``, in a channel table's Total row);
* ``FIX`` rewritten or marked (left byte-for-byte alone is a miss);
* ``LEFT`` no projection in the turn and nothing marks the reply as a plan
  answer: left alone.

Each case is run twice: through ``nova_plan_figures.enforce`` and through the whole
``Nova._apply_t3_postprocessing`` pipeline with the guard switched on and off.
"""

from __future__ import annotations

import json
import re
import sys
import time
import types
from pathlib import Path
from typing import Dict, List, Tuple

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
# frozen output of the real projection tool (the corpus must not drift with the engine)
PROJ = {
    "g60": json.loads((FIXTURES / "projection_60k_generic.json").read_text("utf-8")),
    "g120": json.loads((FIXTURES / "projection_120k_generic.json").read_text("utf-8")),
    "h50": json.loads(
        (FIXTURES / "projection_50k_nurses_houston.json").read_text("utf-8")
    ),
}
BRIEF_TOOLS = {
    "": (),
    "g60": ("g60",),
    "h50": ("h50",),
    "g120": ("g120",),
    "g60+g120": ("g60", "g120"),
}

# (id, category, brief, earlier user turns, user message, answer text, expectation)
VERIFIER_CASES = [
    (
        "a1",
        "a",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Expect $1.7k–$3.4k cost per hire on this plan.",
        "OK",
    ),
    (
        "a2",
        "a",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Cost per hire runs 1.7K to 3.4K dollars for this plan.",
        "OK",
    ),
    (
        "a3",
        "a",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "The plan should bring in ~2,400 applications at the low end.",
        "OK",
    ),
    (
        "a4",
        "a",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan projects 17 to 35 hires.",
        "OK",
    ),
    (
        "a5",
        "a",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan projects 17–35 placements.",
        "OK",
    ),
    (
        "a6",
        "a",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your budget should deliver between 17 and 35 hires.",
        "OK",
    ),
    (
        "a7",
        "a",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Expect 2.4k–7.5k applications from this campaign.",
        "OK",
    ),
    (
        "a8",
        "a",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "At $8 to $25 per application, the plan yields 2,400 to 7,500 applications.",
        "OK",
    ),
    (
        "a9",
        "a",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "For your nurses, the plan projects 4–9 hires at $5,250–$10,500 per hire.",
        "OK",
    ),
    (
        "a10",
        "a",
        "g120",
        (),
        "$120k for 3 months, customer service reps in Chicago",
        "At $120k the plan projects 35–71 hires and 4,800–15,000 applications.",
        "OK",
    ),
    (
        "a11",
        "a",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Expect **17–35 hires** and **2,400–7,500 applications**.",
        "OK",
    ),
    (
        "a12",
        "a",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "The campaign should generate about 7,500 applications.",
        # was "R" (rewrite the best-case end to the range); 7,500 IS the engine's
        # high end, so under mark-only it traces and gains the basis line instead
        "OK",
    ),
    (
        "a13",
        "a",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Applications from the plan: 2,400 through 7,500.",
        "OK",
    ),
    (
        "a14",
        "a",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your plan delivers somewhere from 2,400 to 7,500 applications.",
        "OK",
    ),
    (
        "a15",
        "a",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan projects 17-35 hires, i.e. roughly 6-12 hires per month.",
        "OK",
    ),
    (
        "a16",
        "a",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "The plan's 17–35 hires assume a 90-day time to fill.",
        "OK",
    ),
    (
        "b1",
        "b",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "The median salary for registered nurses in Houston is $84,000 (BLS).",
        "U",
    ),
    (
        "b2",
        "b",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "BLS counts 3,175,390 registered nurses employed nationally.",
        "U",
    ),
    (
        "b3",
        "b",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "Indeed currently lists 1,240 postings for RNs in Houston.",
        "U",
    ),
    (
        "b4",
        "b",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "Healthcare ads convert at an apply rate of 4.1%.",
        "U",
    ),
    (
        "b5",
        "b",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "The JOLTS quits rate for healthcare is 2.3% a month.",
        "U",
    ),
    (
        "b6",
        "b",
        "h50",
        (),
        "We need 14 nurses starting March, $50k for 3 months in Houston",
        "To fill your 14 hires by March, start sourcing in January.",
        "U",
    ),
    (
        "b7",
        "b",
        "h50",
        (),
        "plan $50k for 3 months; headcount of 14 RNs in Houston",
        "With a headcount of 14, the plan would cover all 14 hires only at the best case.",
        "U",
    ),
    (
        "b8",
        "b",
        "h50",
        (),
        "We're adding 14 FTEs, budget $50k, 3 months, nurses Houston",
        "You are adding 14 FTEs, so 14 hires is the target for this campaign.",
        "U",
    ),
    (
        "b9",
        "b",
        "g60",
        (),
        "We want to hire 50 people with $60k over 3 months, CSRs in Chicago",
        "Hiring 50 people will take more than this budget: the plan delivers 17–35 hires.",
        "U",
    ),
    (
        "b9b",
        "b",
        "g60",
        (),
        "We want to hire 50 people with $60k over 3 months, CSRs in Chicago",
        "50 hires is above what this $60k plan buys.",
        "U",
    ),
    (
        "b10",
        "b",
        "g60",
        (),
        "our 14-person team needs 6 more CSRs, $60k 3 months Chicago",
        "Your 14-person team can absorb the 6 hires you need.",
        "U",
    ),
    (
        "b11",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "In Germany, Indeed CPCs run €0.45 and LinkedIn about €2.10.",
        "U",
    ),
    (
        "b12",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "In the UK, a typical CPA is £12 per application.",
        "U",
    ),
    (
        "b13",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "In India, Naukri averages ₹180 per application.",
        "U",
    ),
    (
        "b14",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "In Japan, CPA averages ¥2,800.",
        "U",
    ),
    (
        "b15",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Walmart hires about 40,000 seasonal workers each fall.",
        "U",
    ),
    (
        "b16",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your 2026 hires should be onboarded by February.",
        "U",
    ),
    (
        "b16b",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Plan for Q1 2027 hires now.",
        "U",
    ),
    (
        "b17",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "In the first month, expect about 5 hires as campaigns ramp.",
        "NR",
    ),
    (
        "b18",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Week 1 typically brings 300 applications.",
        "NR",
    ),
    (
        "b19",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Plan on 1 hire per 57 applications.",
        "U",
    ),
    (
        "b20",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your hire rate of 2.3% is in line with the plan.",
        "U",
    ),
    (
        "b21",
        "b",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "- Median RN salary: $84,000\n- Active RN postings in Houston: 1,240\n- Average time to fill: 49 days",
        "U",
    ),
    (
        "b22",
        "b",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "| Metric | Value |\n|---|---|\n| Median salary | $84,000 |\n| Postings | 1,240 |",
        "U",
    ),
    (
        "b23",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "### 3 hires per recruiter per month is the norm",
        "U",
    ),
    (
        "b24",
        "b",
        "g60",
        (),
        "we need 14 hires; $60k 3 months CSR Chicago",
        "Your goal of **14 hires** fits inside the plan's 17–35 hires range.",
        "U",
    ),
    (
        "b25",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        'Your ad headline "Join 200 new hires this spring" tested well.',
        "U",
    ),
    (
        "b26",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Source: SHRM 2025 benchmark, $5,475 cost per hire[^1].",
        "U",
    ),
    (
        "b27",
        "b",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "(For context, Houston hospitals posted 2,100 RN openings last quarter.)",
        "U",
    ),
    (
        "b28",
        "b",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "Indeed has 1.2M postings for nursing roles.",
        "U",
    ),
    (
        "b28b",
        "b",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "LinkedIn has 1B members worldwide.",
        "U",
    ),
    (
        "b29",
        "b",
        "g60",
        (),
        "we got 900 applications from our career site last year; $60k 3 months CSR Chicago",
        "Your career site's 900 applications last year show strong organic demand.",
        "U",
    ),
    (
        "b30",
        "b",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "Hospitals typically spend $10,000+ per hire for travel nurses.",
        "U",
    ),
    (
        "b31",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Our data shows 62% of applications come from mobile.",
        "U",
    ),
    (
        "b32",
        "b",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "Nurses submit 3.4 applications on average before accepting an offer.",
        "U",
    ),
    (
        "b33",
        "b",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "A single RN requisition draws about 40 applications in Houston.",
        "U",
    ),
    (
        "b34",
        "b",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Expect your recruiters to screen 30 applications a day.",
        "U",
    ),
    (
        "c1",
        "c",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Here is your plan: about 900 applications, 14 hires, and a CPA of $66.67.",
        "R",
    ),
    (
        "c2",
        "c",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "**Projected results for this plan**\n- Applications: 900\n- Hires: 14\n- CPA: $66.67",
        "FIX",
    ),
    (
        "c3",
        "c",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "| Metric | Projection |\n|---|---|\n| Applications | 900 |\n| Hires | 14 |\n| CPA | $66.67 |",
        "FIX",
    ),
    (
        "c4",
        "c",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan should deliver **14 hires** and **900 applications**.",
        "R",
    ),
    (
        "c5",
        "c",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan should deliver fourteen hires and nine hundred applications.",
        "FIX",
    ),
    (
        "c6",
        "c",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan projects ~917 applications and 17 hires at $65.43 avg CPA.",
        "R",
    ),
    (
        "c7",
        "c",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your $60k budget delivers 96 hires at $625 per hire.",
        "R",
    ),
    (
        "c8",
        "c",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "At $60k a month, this campaign would deliver 51–105 hires.",
        "FIX",
    ),
    (
        "c9",
        "c",
        "",
        (),
        "what's a good number for nurses in Houston?",
        "Expect about 14 hires and 900 applications at a CPA of $66.67.",
        "FIX",
    ),
    (
        "c10",
        "c",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "### 14 hires projected for this plan\nDetails below.",
        "FIX",
    ),
    (
        "c11",
        "c",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Total projected hires: 14. Total applications: 900.",
        "R",
    ),
    (
        "c12",
        "c",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "For this budget, expect 14 hires at a cost per hire of $3,571.",
        "R",
    ),
    (
        "c13",
        "c",
        "g120",
        (),
        "$120k for 3 months, customer service reps in Chicago",
        "Expect ~1,834 applications and 34 hires from this $120k plan.",
        "R",
    ),
    (
        "c14",
        "c",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "The campaign yields 900 apps and 14 hires.",
        "FIX",
    ),
    (
        "c15",
        "c",
        "g60",
        (),
        "our team of 14 recruiters needs a $60k 3-month CSR plan in Chicago",
        "This plan delivers 14 hires and 900 applications.",
        "FIX",
    ),
    (
        "c16",
        "c",
        "h50",
        (),
        "plan $50k over 3 months for registered nurses in Houston",
        "With $50k you can expect roughly 30 hires.",
        "R",
    ),
    (
        "c17",
        "c",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "I estimate the plan returns 1,200 applications; that is about 20 hires.",
        "R",
    ),
    (
        "d1",
        "d",
        "g60+g120",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago and compare with $120k",
        "At $60k the plan projects 17–35 hires; at $120k it projects 35–71 hires.",
        "OK",
    ),
    (
        "d2",
        "d",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "At $60k you get 17–35 hires; doubling to $120k gets you roughly 35–71 hires.",
        "NR",
    ),
    (
        "d3",
        "d",
        "",
        (),
        "what if I double it?",
        "Doubling to $120k would roughly double that range, to about 35–71 hires.",
        "NR",
    ),
    (
        "d4",
        "d",
        "",
        (),
        "and how many hires was that again?",
        "As above, the plan projects 17–35 hires.",
        "U",
    ),
    (
        "d5",
        "d",
        "h50",
        (),
        "now do the nurses plan: $50k, 3 months, Houston",
        "Compared with the $60k CSR plan (17–35 hires), your nurse plan projects 4–9 hires.",
        "NR",
    ),
    (
        "e1",
        "e",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Le plan prévoit 1.500 candidatures.",
        "U",
    ),
    (
        "e2",
        "e",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan projects 2 400 applications at the low end.",
        "NR",
    ),
    (
        "e3",
        "e",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Germany has 1,5 Mio. open positions.",
        "U",
    ),
    (
        "e4",
        "e",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan projects 7.500 applications at the high end.",
        "NR",
    ),
    (
        "g1",
        "g",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "## Hires\n14 recruiters will support the 3 hiring managers.",
        "U",
    ),
    (
        "g2",
        "g",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Use Hires.com to reach 250,000 candidates.",
        "U",
    ),
    (
        "g3",
        "g",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your ATS integrates with 12 applications.",
        "U",
    ),
    (
        "g3b",
        "g",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "The client runs 12 applications on Workday.",
        "U",
    ),
    (
        "g3c",
        "g",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your bank processes 4,000 loan applications a month, so the plan targets tellers.",
        "U",
    ),
    (
        "g4",
        "g",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your CPA at $250 an hour can review the budget.",
        "U",
    ),
    (
        "g4b",
        "g",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Senior CPA: $95,000 salary in your budget.",
        "U",
    ),
    (
        "g5",
        "g",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Cost per hire is total recruiting spend divided by hires; for example, $10,000 spent for 5 hires is $2,000 per hire.",
        "U",
    ),
    (
        "g6",
        "g",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your career site logged 1,200 apply-starts last month.",
        "U",
    ),
    (
        "g7",
        "g",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan uses 6 channels and 3 job boards.",
        "U",
    ),
    (
        "g8",
        "g",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Hire 3 recruiters to process the plan's applications.",
        "U",
    ),
    (
        "v1",
        "v",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "To reach 50 hires, you'd need about $150k.",
        "NR",
    ),
    (
        "v2",
        "v",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Expect roughly 45 applications for each hire.",
        "U",
    ),
    (
        "v3",
        "v",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Agencies bill 20% of salary, about $8,200 per hire.",
        "U",
    ),
    (
        "v7",
        "v",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Hiring managers interview 5 applicants per hire.",
        "U",
    ),
    (
        "v8",
        "v",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "The plan yields 3x more applications than last year.",
        "U",
    ),
    (
        "v11",
        "v",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "With 2 recruiters, each handling 9 hires, your plan is covered.",
        "U",
    ),
    (
        "v12",
        "v",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Of those, plan for 10 new hires to be bilingual.",
        "NR",
    ),
    (
        "v13",
        "v",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Budget 3 hires for the night shift out of the 17–35 total.",
        "U",
    ),
    (
        "v14",
        "v",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Expect 2 hires from the referral program, outside this plan.",
        "NR",
    ),
    (
        "v16",
        "v",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "For your 3 locations, expect 2,400–7,500 applications in total.",
        "OK",
    ),
    (
        "v17",
        "v",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan covers 14 roles across 3 sites.",
        "U",
    ),
    (
        "v18",
        "v",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "You'll need 3 interviewers for every 10 hires.",
        "U",
    ),
    (
        "v19",
        "v",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your recruiting team made 120 hires in 2025 with a similar budget.",
        "U",
    ),
    (
        "v20",
        "v",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Each recruiter can close about 8 hires over the campaign.",
        "U",
    ),
    (
        "v21",
        "v",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your 17–35 hires would cost $1,675–$3,350 each; Indeed's sponsored jobs average $0.80 per click.",
        "OK",
    ),
    (
        "h1",
        "h",
        "h50",
        (),
        "We need 14 nurses starting March, $50k for 3 months in Houston",
        "To have 14 nurses in place by March, the plan projects 4–9 hires, so plan a second wave.",
        "OK",
    ),
    (
        "h2",
        "h",
        "h50",
        (),
        "We need 14 nurses starting March, $50k for 3 months in Houston",
        "Your 14 openings exceed the 4–9 hires this plan projects.",
        "OK",
    ),
    (
        "h3",
        "h",
        "h50",
        (),
        "We need 14 nurses starting March, $50k for 3 months in Houston",
        "The plan projects 4–9 hires against your 14 hires needed.",
        "OK",
    ),
    (
        "h4",
        "h",
        "h50",
        (),
        "plan $50k for 3 months; headcount of 14 RNs in Houston",
        "Against a headcount of 14, this budget yields 4–9 hires.",
        "OK",
    ),
    (
        "h5",
        "h",
        "h50",
        (),
        "We need fourteen nurses; $50k, 3 months, Houston",
        "Fourteen nurses means 14 hires, above the plan's 4–9 hires.",
        "NR",
    ),
    (
        "u2",
        "h",
        "g60",
        (),
        "Budget 60k, 3 months, CSR in Chicago; we want fifteen hires",
        "This plan projects 17–35 hires, comfortably above your fifteen.",
        "OK",
    ),
    (
        "u3",
        "h",
        "g60",
        (),
        "$60k, 3 months, Chicago CSRs, we have 25 openings",
        "Your plan should fill all 25 openings with 17–35 hires projected.",
        "OK",
    ),
    (
        "mt1",
        "multiturn",
        "h50",
        ("We need 14 nurses in Houston starting in March.",),
        "ok, $50k for 3 months",
        "Your 14 hires are scheduled to start in March; the plan projects 4–9 hires.",
        "U",
    ),
    (
        "mt2",
        "multiturn",
        "h50",
        ("We need 14 nurses in Houston starting in March.",),
        "now make it $50k over 3 months",
        "For your 14 hires starting in March, this plan projects 4–9 hires.",
        "U",
    ),
    (
        "mt3",
        "multiturn",
        "g60",
        ("We have 25 openings across both sites.",),
        "budget is $60k for 3 months",
        "This plan covers your 25 hires across both sites.",
        "NR",
    ),
    (
        "mt4",
        "multiturn",
        "g60",
        ("Last year our team made 120 hires.",),
        "so how does this plan compare?",
        "Your team made 120 hires in 2025; this plan projects 17–35 hires.",
        "U",
    ),
    (
        "mt5",
        "multiturn",
        "g60",
        ("We got 900 applications last campaign.",),
        "and with $60k over 3 months?",
        "Last campaign brought you 900 applications; this plan projects 2,400–7,500 applications.",
        "U",
    ),
    (
        "cpa_role1",
        "probe",
        "g60",
        (),
        "make a $60k 3-month plan to hire CPAs in Chicago",
        "A licensed CPA in Chicago earns about $95,000, so your budget should lean on niche boards.",
        "U",
    ),
    (
        "cpa_role2",
        "probe",
        "g60",
        (),
        "make a $60k 3-month plan to hire CPAs in Chicago",
        "Your CPA hires will expect a salary of $90,000 to $110,000.",
        "U",
    ),
    (
        "cpa_role3",
        "probe",
        "g60",
        (),
        "make a $60k 3-month plan to hire CPAs in Chicago",
        "Pay for this plan's CPA roles is $45 to $60 per hour.",
        "U",
    ),
    (
        "cpa_role4",
        "probe",
        "g60",
        (),
        "make a $60k 3-month plan to hire CPAs in Chicago",
        "The CPA exam costs about $1,500, which your plan could reimburse.",
        "U",
    ),
    (
        "cpa_role5",
        "probe",
        "g60",
        (),
        "make a $60k 3-month plan to hire CPAs in Chicago",
        "For this campaign, a CPA with 5 years' experience costs about $105,000.",
        "U",
    ),
    (
        "ratio1",
        "probe",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Expect about 40 applications to fill each role.",
        "U",
    ),
    (
        "ratio2",
        "probe",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "The plan needs roughly 50 applications to make one hire.",
        "U",
    ),
    (
        "ratio3",
        "probe",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Expect around 30 applications behind every hire.",
        "U",
    ),
    (
        "ratio4",
        "probe",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Plan on 60 applications for a single hire.",
        "U",
    ),
    (
        "ratio5",
        "probe",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your plan converts about 1 in 50 applicants, i.e. 50 applications per hire.",
        "U",
    ),
    (
        "sub1",
        "probe",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Of the projected hires, expect about 5 hires to be bilingual.",
        "NR",
    ),
    (
        "sub2",
        "probe",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your plan should yield 8 Spanish-speaking hires.",
        "NR",
    ),
    (
        "hist1",
        "probe",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "In 2025 your team made 120 hires with a similar budget.",
        "U",
    ),
    (
        "hist2",
        "probe",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your Q2 campaign produced 1,100 applications.",
        "U",
    ),
]

OWN_CASES = [
    (
        "own_m1",
        "multiturn",
        "g60",
        ("We're hiring 40 CSRs this spring.",),
        "ok, $60k for 3 months in Chicago",
        "Your 40 hires are well above the plan's 17–35 hires.",
        "OK",
    ),
    (
        "own_m2",
        "multiturn",
        "g60",
        ("Last quarter we got 1,800 applications from Indeed.",),
        "plan $60k, 3 months",
        "Those 1,800 applications came from Indeed alone; this plan projects 2,400–7,500.",
        "OK",
    ),
    (
        "own_m3",
        "multiturn",
        "g60",
        ("We have 6 recruiters, 3 hiring managers and 25 open reqs.",),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your 25 open reqs map to 25 hires, inside the plan's 17–35 range.",
        "OK",
    ),
    (
        "own_m4",
        "multiturn",
        "g60",
        ("We need 14 hires.",),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan delivers 900 applications.",
        "R",
    ),
    (
        "own_m5",
        "multiturn",
        "g60",
        ("We need 14 hires.",),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan delivers 14 hires.",
        "OK",
    ),
    (
        "own_m6",
        "multiturn",
        "g60",
        ("Our target is fifteen hires.",),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan delivers fifteen hires.",
        "OK",
    ),
    (
        "own_m7",
        "multiturn",
        "h50",
        ("We have 14 nurse openings.", "Budget is flexible."),
        "plan $50k over 3 months for registered nurses in Houston",
        "To fill 14 openings you would need roughly 3x this plan; it projects 4–9 hires.",
        "OK",
    ),
    (
        "own_m8",
        "multiturn",
        "g60",
        ("hi", "I run a team of 22 recruiters."),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "With 22 hires you would outgrow this plan, which projects 17–35 hires.",
        "OK",
    ),
    (
        "own_m9",
        "multiturn",
        "g60",
        (),
        "budget is $60k for 3 months",
        "Last quarter's 900 applications are a fraction of what this plan projects, 2,400–7,500.",
        "NR",
    ),
    (
        "own_m10",
        "multiturn",
        "g60",
        ("We got 900 applications last campaign.",),
        "and for this one, $60k over 3 months?",
        "You got 900 applications last campaign; this plan projects 2,400–7,500.",
        "OK",
    ),
    (
        "own_r1",
        "staff",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Hire 3 recruiters and 2 coordinators to run the plan.",
        "U",
    ),
    (
        "own_r2",
        "staff",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Staff the campaign with 3 recruiters; each hire needs 2 interviews.",
        "U",
    ),
    (
        "own_r3",
        "staff",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "A team of 5 recruiters can process the plan's 2,400–7,500 applications.",
        "OK",
    ),
    (
        "own_r4",
        "staff",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your 3 recruiters each close 6 hires over the campaign.",
        "U",
    ),
    (
        "own_r5",
        "staff",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Assign 2 recruiters per 10 hires.",
        "U",
    ),
    (
        "own_r6",
        "staff",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "One recruiter for every 25 hires is typical.",
        "U",
    ),
    (
        "own_r7",
        "staff",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Plan for 3 hires inside the recruiting team itself.",
        "NR",
    ),
    (
        "own_r8",
        "staff",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your team of 14 will screen the 2,400–7,500 applications.",
        "OK",
    ),
    (
        "own_r9",
        "staff",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your 14-person staff handles intake.",
        "U",
    ),
    (
        "own_r10",
        "staff",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Our staffing agency partner quotes $6,000 per hire.",
        "U",
    ),
    (
        "own_r11",
        "staff",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your internal team made 45 hires last year.",
        "U",
    ),
    (
        "own_r12",
        "staff",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Headcount: 14 hires approved.",
        "NR",
    ),
    (
        "own_r13",
        "staff",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Three recruiters, four coordinators and a manager run the desk.",
        "U",
    ),
    (
        "own_u1",
        "range",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Expect $1.7k–$3.4k per hire on this plan.",
        "OK",
    ),
    (
        "own_u2",
        "range",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan should land 2.4k–7.5k applications.",
        "OK",
    ),
    (
        "own_u3",
        "range",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "The plan delivers 17 to 35 hires.",
        "OK",
    ),
    (
        "own_u4",
        "range",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "The plan delivers 17-35 hires (about 6–12 per month).",
        "OK",
    ),
    (
        "own_u5",
        "range",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan projects 20–30 hires.",
        "R",
    ),
    (
        "own_u6",
        "range",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Roughly 2,000–8,000 applications are possible on this plan.",
        "FIX",
    ),
    (
        "own_u7",
        "range",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Budget $60k → expect 900 applications.",
        "FIX",
    ),
    (
        "own_u8",
        "range",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "At $8–$25 per application, 2,400–7,500 applications come in.",
        "OK",
    ),
    (
        "own_u9",
        "range",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan projects 2 400 applications at the low end.",
        "OK",
    ),
    (
        "own_u10",
        "range",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Expect 17 – 35 hires from this plan.",
        "OK",
    ),
    (
        "own_t1",
        "table",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "| Channel | Budget | Applications | Hires |\n|---|---|---|---|\n| Indeed | $20,000 | 503 | 4 |\n| Total | $60,000 | 900 | 14 |",
        "R",
    ),
    (
        "own_t2",
        "table",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "| Metric | Value |\n|---|---|\n| Total projected hires | 14 |\n| Budget | $60,000 |",
        "R",
    ),
    (
        "own_t3",
        "table",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "| Metric | Value |\n|---|---|\n| Applications per hire | 57 |\n| Apply rate | 4.1% |",
        "U",
    ),
    (
        "own_t4",
        "table",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "| Plan | Hires | Applications |\n|---|---|---|\n| This plan | 17–35 | 2,400–7,500 |",
        "OK",
    ),
    (
        "own_t5",
        "table",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "| Role | Salary |\n|---|---|\n| CSR | $41,000 |",
        "U",
    ),
    (
        "own_t6",
        "table",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "| Channel | Cost per hire |\n|---|---|\n| Indeed | $9,999 |",
        "FIX",
    ),
    (
        "own_t7",
        "table",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "| Channel | Share |\n|---|---|\n| Indeed | 45% |",
        "U",
    ),
    (
        "own_t8",
        "table",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "|  | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| **Total** | **900** | **14** |",
        "R",
    ),
    (
        "own_t9",
        "table",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "| Metric | Value |\n|---|---|\n| Applications | 900 |\n| Hires | 14 |\n| CPA | $66.67 |",
        "FIX",
    ),
    (
        "own_t10",
        "table",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "| Metric | Low | High |\n|---|---|---|\n| Applications | 2,400 | 7,500 |\n| Hires | 17 | 35 |",
        "OK",
    ),
    (
        "own_n1",
        "list",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "1. 900 applications\n2. 14 hires\n3. $66.67 CPA",
        "FIX",
    ),
    (
        "own_n2",
        "list",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "1. Launch programmatic\n2. Add 3 job boards\n3. Review 14 days in",
        "U",
    ),
    (
        "own_n3",
        "list",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "1. Hires: 17–35\n2. Applications: 2,400–7,500",
        "OK",
    ),
    (
        "own_n4",
        "list",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "3. Budget 3 hires for referrals",
        "NR",
    ),
    (
        "own_h1",
        "heading",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "## 17–35 hires projected",
        "OK",
    ),
    (
        "own_h2",
        "heading",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "## 14 hires projected",
        "FIX",
    ),
    (
        "own_h3",
        "heading",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "### Why 3 channels?",
        "U",
    ),
    (
        "own_h4",
        "heading",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "# Plan for 900 applications",
        "FIX",
    ),
    (
        "own_b1",
        "boldlabel",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "- **Applications:** 900\n- **Hires:** 14",
        "FIX",
    ),
    (
        "own_b2",
        "boldlabel",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "- **Cost per hire:** $4,286",
        "FIX",
    ),
    (
        "own_b3",
        "boldlabel",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "**Applications: 2,400–7,500**",
        "OK",
    ),
    (
        "own_q1",
        "quoted",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Try the headline “Join 500 hires this spring”.",
        "U",
    ),
    (
        "own_q2",
        "quoted",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        'Ad copy: "Over 2,000 applications received!"',
        "U",
    ),
    (
        "own_q3",
        "quoted",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Slogan: 'We hired 300 people' works.",
        "U",
    ),
    (
        "own_q4",
        "quoted",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        'The CTA "Apply now: 14 hires needed" tested well.',
        "U",
    ),
    (
        "own_l1",
        "link",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "See [the 900 applications guide](https://example.com/900-applications) now.",
        "U",
    ),
    (
        "own_l2",
        "link",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Source: [SHRM 2025 – $5,475 per hire](https://shrm.org/x).",
        "U",
    ),
    (
        "own_l3",
        "link",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "[Plan details](https://example.com/plan?hires=14&apps=900)",
        "U",
    ),
    (
        "own_l4",
        "link",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Run `hires = 14` against the sheet.",
        "U",
    ),
    (
        "own_d1",
        "locale",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Le plan prévoit 1,5 million de candidatures.",
        "U",
    ),
    (
        "own_d2",
        "locale",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Das Budget liefert 2.400 Bewerbungen.",
        "U",
    ),
    (
        "own_d3",
        "locale",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Este plan proyecta 17 contrataciones.",
        "U",
    ),
    (
        "own_d4",
        "locale",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan projects 2.400 applications.",
        "U",
    ),
    (
        "own_d5",
        "locale",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Budget: 60.000 €, 3 Monate.",
        "U",
    ),
    (
        "own_d6",
        "locale",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan projects 7\xa0500 applications at the high end.",
        "OK",
    ),
    (
        "own_d7",
        "locale",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Le plan prévoit 2\xa0400 candidatures et 17 recrutements.",
        "U",
    ),
    (
        "own_x1",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Nova projects 14 hires for this budget.",
        "R",
    ),
    (
        "own_x2",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Bottom line: with $60k you should land 14 hires.",
        "R",
    ),
    (
        "own_x3",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "The plan's projected result: 14 hires.",
        "FIX",
    ),
    (
        "own_x4",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your $60k budget yields about 14 hires and 900 applications.",
        "R",
    ),
    (
        "own_x5",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Our estimate for this campaign is 14 hires.",
        "FIX",
    ),
    (
        "own_x6",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "At this budget, you get 14 hires.",
        "R",
    ),
    (
        "own_x7",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "For $60,000 you get roughly 96 hires.",
        "R",
    ),
    (
        "own_x8",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Plan: 900 applications, 14 hires.",
        "FIX",
    ),
    (
        "own_x8b",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Plan: 900 applications / 14 hires.",
        "U",
    ),
    (
        "own_x9",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Plan A projects 17–35 hires; Plan B projects 40–60 hires.",
        "NR",
    ),
    (
        "own_x10",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Compared with last quarter's 900 applications, this plan projects 2,400–7,500.",
        "NR",
    ),
    (
        "own_x11",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Expect 14 hires.",
        "FIX",
    ),
    (
        "own_x12",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "If you double the budget, this plan delivers 70 hires.",
        "NR",
    ),
    (
        "own_x13",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan delivers 14 hires by April.",
        "NR",
    ),
    (
        "own_x14",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan delivers 14 hires within 30 days.",
        "NR",
    ),
    (
        "own_x15",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Indeed alone delivers 600 applications on this plan.",
        "NR",
    ),
    (
        "own_x16",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan delivers 900 applications from programmatic.",
        "NR",
    ),
    (
        "own_x17",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan needs 40 hires to hit your goal.",
        "NR",
    ),
    (
        "own_x18",
        "shape",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan delivers at least 40 hires.",
        "NR",
    ),
    (
        "own_e1",
        "noengine",
        "",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Expect about 14 hires.",
        "FIX",
    ),
    (
        "own_e2",
        "noengine",
        "",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan delivers 14 hires.",
        "FIX",
    ),
    (
        "own_e3",
        "noengine",
        "",
        (),
        "what is a CPA?",
        "A CPA is the cost per application, for example $20 for 5 applications.",
        "LEFT",
    ),
    (
        "own_e4",
        "noengine",
        "",
        (),
        "tell me about recruitment advertising",
        "Expect about 14 hires.",
        "LEFT",
    ),
    (
        "own_adv1",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan will not deliver 40 hires.",
        "NR",
    ),
    (
        "own_adv2",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan doesn't reach 40 hires at $60k.",
        "NR",
    ),
    (
        "own_adv3",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "In Chicago, this plan projects 8 hires.",
        "NR",
    ),
    (
        "own_adv4",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "For Registered Nurses the plan delivers 8 hires.",
        "NR",
    ),
    (
        "own_adv5",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan delivers 40+ hires.",
        "NR",
    ),
    (
        "own_adv6",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "The plan converts 40% of 900 applications into hires.",
        "U",
    ),
    (
        "own_adv7",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Half of the 14 hires start in March.",
        "U",
    ),
    (
        "own_adv8",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Slogan: ‘Join 500 hires today’ works.",
        "U",
    ),
    (
        "own_adv9",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "To get 40 hires this plan would need $120k.",
        "NR",
    ),
    (
        "own_adv10",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This budget is not enough for 40 hires.",
        "NR",
    ),
    (
        "own_adv11",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan delivers 900 applications in Phase 1.",
        "NR",
    ),
    (
        "own_adv12",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "The plan delivers 14 hires across the Dallas site.",
        "NR",
    ),
    (
        "own_adv13",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan delivers 17–35 hires; in Chicago that means 8 hires.",
        "NR",
    ),
    (
        "own_adv14",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan delivers 14 hires, 40% more than last year.",
        "NR",
    ),
    (
        "own_adv15",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Nova projects 900 applications.",
        "R",
    ),
    (
        "own_adv16",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Your plan will yield 900 applications.",
        "R",
    ),
    (
        "own_adv17",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Our 60k plan projects 14 hires.",
        "R",
    ),
    (
        "own_adv18",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "The $60k campaign should return 14 hires.",
        "R",
    ),
    (
        "own_adv19",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "With this budget, expect to hire 14 people.",
        "U",
    ),
    (
        "own_adv20",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "This plan delivers 14 hires, 900 applications and 3 channels.",
        "R",
    ),
    (
        "own_adv21",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Without more budget, the plan cannot reach 40 hires.",
        "NR",
    ),
    (
        "own_adv22",
        "adversarial",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago",
        "Is 14 hires realistic for this plan?",
        "U",
    ),
]

ALL_CASES = VERIFIER_CASES + OWN_CASES
assert len({c[0] for c in ALL_CASES}) == len(ALL_CASES) == 131 + len(OWN_CASES)
assert len(VERIFIER_CASES) == 131 and len(OWN_CASES) >= 60

# The verifier expected c9 ("what's a good number for nurses in Houston?" answered
# "Expect about 14 hires and 900 applications at a CPA of $66.67." with NO tool call)
# to be rewritten or marked. It is left alone on purpose: no projection ran and
# neither the question nor the answer is a plan, which is also what the first
# corpus's v2_no_tool_nonplan pins. Recorded as a known partial, not hidden.
KNOWN_PARTIAL = {"c9"}

_APPENDIX_STARTS = (
    "\n\n*Projection basis:",
    "\n\n**Projected outcomes (Nova planning engine)**",
)


def _plan_context(brief: str, message: str) -> bool:
    """What Nova passes: the reply is a plan chat when a budget is in the message."""
    return bool(brief) or bool(
        re.search(r"\$\s?\d+\s?[kK]\b|\$\d{1,3}(?:,\d{3})+", message)
    )


def _raws(brief: str) -> List[str]:
    return [json.dumps(PROJ[name]) for name in BRIEF_TOOLS[brief]]


def _split(out: str) -> Tuple[str, str]:
    cuts = [out.find(m) for m in _APPENDIX_STARTS if out.find(m) >= 0]
    cut = min(cuts) if cuts else len(out)
    return out[:cut], out[cut:]


def _enforce(case, text=None):
    cid, _cat, brief, turns, msg, body, _exp = case
    return pf.enforce(
        body if text is None else text,
        _raws(brief),
        msg,
        plan_context=_plan_context(brief, msg),
        plan_shaped=False,
        user_turns=list(turns),
    )


def _verdict(exp: str, cid: str, text: str, core: str, rewritten: bool, marked: bool):
    """Assert one expectation. ``core`` is the answer without the guard's
    appendices; ``rewritten`` / ``marked`` say what the guard did."""
    undagger = core.replace(pf.DAGGER, "")
    if exp in ("U", "OK", "LEFT"):
        assert not rewritten, f"{cid}: WRONGFUL REWRITE {core!r}"
        assert not marked, f"{cid}: wrongful mark {core!r}"
        assert undagger == text
    elif exp == "NR":
        assert not rewritten, f"{cid}: WRONGFUL REWRITE {core!r}"
        assert undagger == text, f"{cid}: changed beyond a dagger: {core!r}"
    elif exp == "R":
        assert marked, f"{cid}: the wrong plan total was left: {core!r}"
        assert not rewritten, f"{cid}: the guard never rewrites: {core!r}"
    else:
        assert exp == "FIX"
        if cid in KNOWN_PARTIAL:
            assert not rewritten and undagger == text
        else:
            assert rewritten or marked, f"{cid}: left alone: {core!r}"


@pytest.mark.parametrize("case", ALL_CASES, ids=[c[0] for c in ALL_CASES])
def test_corpus2_case(case):
    cid, _cat, _brief, _turns, _msg, text, exp = case
    out, stats = _enforce(case)
    core, tail = _split(out)
    _verdict(exp, cid, text, core, stats["rewritten"] > 0, stats["marked"] > 0)
    assert stats["rewritten"] == 0
    # the only things the guard may add after the answer are its own lines
    for piece in [p for p in tail.split("\n\n") if p.strip()]:
        assert piece.startswith(
            (
                "*Projection basis:",
                "**Projected outcomes",
            )
        )


@pytest.mark.parametrize("case", ALL_CASES, ids=[c[0] for c in ALL_CASES])
def test_corpus2_case_is_idempotent(case):
    once, _ = _enforce(case)
    twice, stats = _enforce(case, once)
    assert twice == once and stats["rewritten"] == 0


@pytest.mark.parametrize("case", ALL_CASES, ids=[c[0] for c in ALL_CASES])
def test_a_number_the_user_typed_is_never_removed(case):
    """Whatever the guard does, every count the user typed (in any turn) that
    appears in the answer is still there: it can gain a dagger, never lose digits."""
    _cid, _cat, _brief, turns, msg, text, _exp = case
    out, _ = _enforce(case)
    core = _split(out)[0].replace(pf.DAGGER, "")
    typed = set()
    for turn in list(turns) + [msg]:
        typed |= {
            m.group(0)
            for m in re.finditer(
                r"(?<![\w$.,])\d{1,3}(?:,\d{3})+(?![\w.,]?\d)|(?<![\w$.,])\d{2,}(?![\w.,]?\d)",
                turn,
            )
        }
    for token in typed:
        before = len(re.findall(rf"(?<![\d,.]){re.escape(token)}(?![\d]|,\d)", text))
        after = len(re.findall(rf"(?<![\d,.]){re.escape(token)}(?![\d]|,\d)", core))
        assert after >= before, f"{case[0]}: the user's {token} was removed"


# ---------------------------------------------------------------------------
# The same corpus through the whole Nova pipeline (legacy verifier included)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def engine():
    return nova.Nova()


@pytest.fixture(autouse=True)
def _flags(monkeypatch):
    monkeypatch.setenv("NOVA_NUMBER_VERIFIER_ENABLED", "1")
    monkeypatch.setenv("NOVA_CITATIONS_ENABLED", "0")


def _t3(engine_, case, guard: bool, monkeypatch):
    _cid, _cat, brief, turns, msg, text, _exp = case
    monkeypatch.setenv("NOVA_PLAN_FIGURE_GUARD_ENABLED", "1" if guard else "0")
    nova._set_turn_tool_capture(
        {
            "conversation_id": "",
            "tools": {},
            "tools_used": [],
            "stateless": True,
            "user_turns": list(turns) + [msg],
        }
    )
    try:
        result = {
            "response": text,
            "sources": [],
            "confidence": 0.8,
            "tools_used": ["query_budget_projection"] if brief else [],
            "_tool_results_raw": _raws(brief),
        }
        return engine_._apply_t3_postprocessing(result, msg)["response"]
    finally:
        nova._set_turn_tool_capture(None)


def _strip(s: str) -> str:
    s = s.replace(nova._T3_UNVERIFIED_FOOTER, "")
    s = re.sub(r"\*Projection basis:[^\n]*\*", "", s)
    s = s.split("**Projected outcomes (Nova planning engine)**")[0]
    return s.replace("<!-- t3-numver -->", "").strip()


@pytest.mark.parametrize("case", ALL_CASES, ids=[c[0] for c in ALL_CASES])
def test_corpus2_case_through_the_pipeline(engine, monkeypatch, case):
    """What the guard itself did, isolated from the legacy $ / % / K verifier by
    running the pipeline with the guard on and off."""
    cid, _cat, _brief, _turns, _msg, text, exp = case
    on = _t3(engine, case, True, monkeypatch)
    off = _t3(engine, case, False, monkeypatch)
    core_on, core_off = _strip(on), _strip(off)
    rewritten = core_on.replace(pf.DAGGER, "") != core_off.replace(pf.DAGGER, "")
    marked = on.count(pf.DAGGER) > off.count(pf.DAGGER)
    if exp == "FIX" and pf.DAGGER in on:
        # the legacy verifier daggers every unverified \$ figure with the guard off
        # too, so the guard's own mark cannot show as an extra dagger
        marked = True
    _verdict(exp, cid, _strip(off).replace(pf.DAGGER, ""), core_on, rewritten, marked)


def test_the_whole_corpus_has_no_wrongful_rewrite_and_few_marks():
    """The verifier's scoreboard: W (wrongful rewrites) = 0; M (marks on figures
    that were true) stays small; X (misses) is only the recorded partial."""
    w = m = x = 0
    for case in ALL_CASES:
        cid, _cat, _brief, _turns, _msg, text, exp = case
        out, stats = _enforce(case)
        core = _split(out)[0]
        if exp in ("U", "OK", "NR", "LEFT"):
            w += stats["rewritten"] > 0
            m += exp in ("U", "OK", "LEFT") and stats["marked"] > 0
        elif exp == "R":
            x += stats["rewritten"] == 0 and stats["marked"] == 0
            w += stats["rewritten"] > 0
        else:
            x += stats["rewritten"] == 0 and stats["marked"] == 0
    assert (w, m) == (0, 0)
    assert x == len(KNOWN_PARTIAL)


# ---------------------------------------------------------------------------
# Every user turn is protected, through Nova's own turn state
# ---------------------------------------------------------------------------
def test_user_turn_texts_collects_every_user_message_and_no_assistant_message():
    history = [
        {"role": "user", "content": "We need 14 hires."},
        {"role": "assistant", "content": "Expect 900 applications."},
        {"role": "user", "content": [{"type": "text", "text": "and 25 openings"}]},
    ]
    turns = pf.user_turn_texts("budget is $60k", history)
    assert turns == ["We need 14 hires.", "and 25 openings", "budget is $60k"]
    state = nova._new_turn_state(
        "budget is $60k", history, session_id="s", stateless=False
    )
    assert state["user_turns"] == turns


def test_an_assistant_figure_from_an_earlier_turn_is_not_the_users(engine, monkeypatch):
    """Only USER turns are pooled: the model's own earlier 900 stays the model's."""
    monkeypatch.setenv("NOVA_PLAN_FIGURE_GUARD_ENABLED", "1")
    history = [
        {"role": "assistant", "content": "Expect 900 applications."},
        {"role": "user", "content": "hi"},
    ]
    nova._set_turn_tool_capture(
        nova._new_turn_state("budget is $60k for 3 months", history, session_id="")
    )
    try:
        out = engine._apply_t3_postprocessing(
            {
                "response": "This plan delivers 900 applications.",
                "sources": [],
                "confidence": 0.8,
                "tools_used": ["query_budget_projection"],
                "_tool_results_raw": _raws("g60"),
            },
            "budget is $60k for 3 months",
        )["response"]
    finally:
        nova._set_turn_tool_capture(None)
    assert "900† applications" in out  # marked: the model's figure, not the user's


# ---------------------------------------------------------------------------
# Tables: the layout never changes the verdict
# ---------------------------------------------------------------------------
def test_a_metric_value_table_is_checked_like_the_same_bullets():
    case = ("tb", "table", "g60", (), "plan $60k, 3 months, CSRs in Chicago", "", "FIX")
    table = "| Metric | Value |\n|---|---|\n| Applications | 900 |\n| Hires | 14 |\n| CPA | $66.67 |"
    bullets = "- Applications: 900\n- Hires: 14\n- CPA: $66.67"
    out_t, st_t = _enforce(case, table)
    out_b, st_b = _enforce(case, bullets)
    assert (
        st_t["marked"] == st_b["marked"] == 3
        and st_t["rewritten"] == st_b["rewritten"] == 0
    )
    for fig in ("900†", "14†", "$66.67†"):
        assert fig in out_t and fig in out_b


def test_a_totals_row_is_marked_like_every_other_row():
    case = (
        "tb2",
        "table",
        "g60",
        (),
        "plan $60k, 3 months, CSRs in Chicago",
        "",
        "FIX",
    )
    table = (
        "| Channel | Applications | Hires |\n|---|---|---|\n"
        "| Indeed | 503 | 4 |\n| Total | 900 | 14 |"
    )
    out, st = _enforce(case, table)
    core = _split(out)[0]
    # was rewritten to the engine's range until 2026-10-18: tables are mark-only
    assert "| Indeed | 503† | 4† |" in core
    assert "| Total | 900† | 14† |" in core
    assert st["rewritten"] == 0


# ---------------------------------------------------------------------------
# Linear time: the regexes must not blow up on a long answer
# ---------------------------------------------------------------------------
def test_a_long_answer_with_hundreds_of_numbers_is_checked_in_linear_time():
    sentence = (
        "Programmatic yields 612 applications at a $18 CPA. This plan delivers 14 hires. "
        "The median salary is $41,000 and 1,200 postings are live. "
    )
    case = ("perf", "perf", "g60", (), "plan $60k, 3 months", "", "FIX")
    short, long_ = sentence * 40, sentence * 160
    t0 = time.perf_counter()
    _enforce(case, short)
    t_short = time.perf_counter() - t0
    t0 = time.perf_counter()
    _enforce(case, long_)
    t_long = time.perf_counter() - t0
    assert t_long < max(8 * t_short, 0.25) and t_long < 3.0
    digits = "1" * 20000
    t0 = time.perf_counter()
    pf.enforce(
        f"This plan delivers {digits} hires and ${digits} per hire.",
        _raws("g60"),
        "plan $60k",
        plan_context=True,
    )
    assert time.perf_counter() - t0 < 1.0
