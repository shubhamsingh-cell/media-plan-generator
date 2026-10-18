"""Third corpus for the plan-figure guard (``nova_plan_figures.enforce``), and the
behaviour every corpus is held to.

POLICY (2026-10-18): MARK ONLY, NEVER REWRITE. Three adversarial rounds each
found the guard rewriting true or conditional figures into the engine's total
("For supervisors, the plan delivers 5 hires" -> "17-35 hires"), and a fourth
found the last rewrite, a channel table's exact "Total" row, firing on totals of
something else (a "Monthly hires" column, a table under "### Chicago only", a
second "If you double the budget" table). A hires / applications / CPA /
cost-per-hire figure that traces to no tool result of the turn and to no number
the user typed (any turn) gets a dagger, in prose and in tables; no text is ever
replaced. A row labelled as a total is checked against the plan's own figures.

``CASES`` are the 175 cases of the third independent verifier, with the
expectation codes it wrote BEFORE running them:

* ``U``   untouched (a mark is a false mark, F);
* ``NR``  never rewritten (a mark is fine);
* ``R``   a wrong plan total (the verifier accepted a mark under mark-only);
* ``FIX`` a wrong figure: marked;
* ``MK``  a wrong subset / conditional figure: marked, never rewritten;
* ``MP``  a wrong total equal to a number the user typed: untouched BY POLICY
  (the user's number is theirs; the guard cannot tell the echo from the forecast).

``FRESH_CASES`` are the 62 cases of that verifier's second round (35 Total rows,
27 prose), with its codes: ``R`` (a wrong exact Total row: now MARKED), ``NRW``
(never rewritten), ``U``, ``FIX``, ``MP``, ``ANY`` (no mangling).

Publisher names in two cases were replaced by "Acme Jobs" / "Acme Board" (public
repo); the cases' meaning (a publisher outside the channel words) is unchanged.

The scoreboard test runs ALL corpora (these two sets, ``test_nova_plan_guard_corpus``
and ``test_nova_plan_guard_corpus2``): W (any rewrite) and K (mangled output,
non-idempotent or slow) must be zero; every remaining F and M is listed below
with its reason. Known limits (no word lists): answers in another language; a
channel word in the clause excusing a figure equal to any channel's own; a rate
whose clause does not name the plan. See ``ACCEPTED_M``.
"""

from __future__ import annotations

import itertools
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
import nova_cache  # noqa: E402
import nova_plan_figures as pf  # noqa: E402
from nova_handoff import _CHANNEL_TABLE_RE, is_plan_shaped, parse_budget  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures" / "nova_plan_guard"
_RAW = {
    k: (FIXTURES / f"projection_{v}.json").read_text("utf-8")
    for k, v in (
        ("g60", "60k_generic"),
        ("g120", "120k_generic"),
        ("h50", "50k_nurses_houston"),
    )
}
# the turn's tool results, by brief: frozen projections, and broken results
BRIEFS = {
    "": [],
    "g60": [_RAW["g60"]],
    "g120": [_RAW["g120"]],
    "h50": [_RAW["h50"]],
    "g60+g120": [_RAW["g60"], _RAW["g120"]],
    "g60dup": [_RAW["g60"], _RAW["g60"]],
    "none": [None],
    "err": ['{"error": "projection failed: budget missing", "ok": false}'],
    "garbage": ["Tool timed out after 30s"],
    "emptytext": [json.dumps({"projection": {"text": {}}, "total_budget": 60000})],
    # the projection with a point total (hires low == high)
    "point": [(FIXTURES / "projection_60k_point.json").read_text("utf-8")],
    "nulltext": [
        json.dumps(
            {
                "projection": {
                    "text": {"hires": None, "applications": None},
                    "hires": {"low": None, "high": None},
                },
                "total_budget": None,
            }
        )
    ],
}
M60 = "make a $60k, 3-month media plan for customer service reps in Chicago"
M60R = "make a $60k, 3-month media plan for customer service reps and supervisors in Chicago"
MH = "plan $50k over 3 months for registered nurses in Houston"
M120 = "$120k for 3 months, customer service reps in Chicago"
TABLE_HDR = "| Metric | Value |\n|---|---|\n"

# (id, category, brief, earlier user turns, user message, answer, expectation, note)
CASES = [
    (
        "cur1",
        "currency",
        "g60",
        (),
        M60,
        "This plan projects 2.4k–7.5k applications.",
        "U",
        "engine in k",
    ),
    (
        "cur2",
        "currency",
        "g60",
        (),
        M60,
        "This plan delivers 1.5k applications.",
        "R",
        "fabricated k total",
    ),
    (
        "cur3",
        "currency",
        "g60",
        (),
        M60,
        "This plan's cost per hire is $1.7K–$3.4K.",
        "U",
        "engine rounded K",
    ),
    (
        "cur4",
        "currency",
        "g60",
        (),
        M60,
        "The plan's CPA is €66.67.",
        "FIX",
        "fabricated CPA in EUR",
    ),
    (
        "cur5",
        "currency",
        "g60",
        (),
        M60,
        "This plan projects 7.500 applications at the high end.",
        "U",
        "EU thousands sep, engine value",
    ),
    (
        "cur6",
        "currency",
        "g60",
        (),
        M60,
        "This plan projects 1.234 applications.",
        "FIX",
        "EU thousands sep, fabricated",
    ),
    (
        "cur7",
        "currency",
        "g60",
        (),
        M60,
        "This plan's cost per hire is US$4,000.",
        "FIX",
        "US$ prefix",
    ),
    (
        "cur8",
        "currency",
        "g60",
        (),
        M60,
        "This plan's CPA works out to USD 66.67.",
        "FIX",
        "USD prefix",
    ),
    (
        "cur9",
        "currency",
        "g60",
        (),
        M60,
        "This plan delivers 9 thousand applications.",
        "FIX",
        "digit + thousand",
    ),
    (
        "cur10",
        "currency",
        "g60",
        (),
        M60,
        "This plan delivers nine hundred applications.",
        "FIX",
        "number words",
    ),
    (
        "cur11",
        "currency",
        "g60",
        (),
        M60,
        "With ₹2 crore the plan delivers 140 hires.",
        "NR",
        "foreign-currency other budget",
    ),
    (
        "cur12",
        "currency",
        "g60",
        (),
        M60,
        "With €200k the plan delivers 80 hires.",
        "NR",
        "EUR other budget",
    ),
    (
        "cur13",
        "currency",
        "g60",
        (),
        M60,
        "At £120,000 the campaign yields 60 hires.",
        "NR",
        "GBP other budget",
    ),
    (
        "cur14",
        "currency",
        "g60",
        (),
        M60,
        "With a 120k budget, the plan would deliver about 70 hires.",
        "NR",
        "other budget no $ sign",
    ),
    (
        "cur15",
        "currency",
        "g60",
        (),
        M60,
        "At USD 120,000, the plan delivers 70 hires.",
        "NR",
        "other budget USD prefix",
    ),
    (
        "cur16",
        "currency",
        "g60",
        (),
        M60,
        "With $120 thousand, the plan delivers 70 hires.",
        "NR",
        "other budget $N thousand",
    ),
    (
        "cur17",
        "currency",
        "g60",
        (),
        M60,
        "This plan's CPA is $66.67/application.",
        "FIX",
        "slash application",
    ),
    (
        "cur18",
        "currency",
        "g60",
        (),
        M60,
        "This plan's cost per hire is 4,000 dollars.",
        "FIX",
        "dollars word",
    ),
    (
        "scn1",
        "scenario",
        "g60",
        (),
        M60,
        "With twice the budget, the plan would deliver about 70 hires.",
        "NR",
        "doubled budget",
    ),
    (
        "scn2",
        "scenario",
        "g60",
        (),
        M60,
        "Doubling the budget, the plan delivers about 70 hires.",
        "NR",
        "doubled budget gerund",
    ),
    (
        "scn3",
        "scenario",
        "g60",
        (),
        M60,
        "With half the budget, the plan delivers 8 hires.",
        "NR",
        "halved budget",
    ),
    (
        "scn4",
        "scenario",
        "g60",
        (),
        M60,
        "Should you double the budget, the plan delivers 70 hires.",
        "NR",
        "should-conditional",
    ),
    (
        "scn5",
        "scenario",
        "g60",
        (),
        M60,
        "With the optimized allocation, the plan delivers 100 hires.",
        "NR",
        "other scenario",
    ),
    (
        "scn6",
        "scenario",
        "g60",
        (),
        M60,
        "In a slow market the plan delivers 10 hires.",
        "NR",
        "scenario",
    ),
    (
        "scn7",
        "scenario",
        "g60",
        (),
        M60,
        "At a $15 CPA the plan delivers 4,000 applications.",
        "NR",
        "conditional on assumed CPA",
    ),
    (
        "scn8",
        "scenario",
        "g60",
        (),
        M60,
        "Under the conservative scenario the plan delivers 10 hires.",
        "NR",
        "scenario",
    ),
    (
        "sub1",
        "subset",
        "g60",
        (),
        M60R,
        "For supervisors, the plan delivers 5 hires.",
        "MK",
        "per-role, lowercase",
    ),
    (
        "sub2",
        "subset",
        "g60",
        (),
        M60,
        "Through job boards the plan delivers 14 hires.",
        "NR",
        "channel subset (true global_boards 14)",
    ),
    (
        "sub3",
        "subset",
        "g60",
        (),
        M60,
        "Through career fairs, the plan delivers 5 hires.",
        "MK",
        "channel subset",
    ),
    (
        "sub4",
        "subset",
        "g60",
        (),
        M60,
        "On Acme Jobs, the plan delivers 5 hires.",
        "MK",
        "publisher subset",
    ),
    (
        "sub5",
        "subset",
        "g60",
        (),
        M60,
        "Through Acme Board the plan delivers 6 hires.",
        "MK",
        "publisher subset",
    ),
    (
        "sub6",
        "subset",
        "g60",
        (),
        M60,
        "Over the first half of the campaign, the plan delivers 8 hires.",
        "MK",
        "time slice",
    ),
    (
        "sub7",
        "subset",
        "g60",
        (),
        M60,
        "By year-end, the plan delivers 14 hires.",
        "MK",
        "time slice",
    ),
    (
        "sub8",
        "subset",
        "g60",
        (),
        M60,
        "Before the holiday peak, the plan delivers 14 hires.",
        "MK",
        "time slice",
    ),
    (
        "sub9",
        "subset",
        "g60",
        (),
        M60,
        "The plan delivers 10 additional hires after the repost.",
        "MK",
        "incremental",
    ),
    (
        "sub10",
        "subset",
        "g60",
        (),
        M60,
        "The plan delivers 600 qualified applications.",
        "MK",
        "qualified subset",
    ),
    (
        "sub11",
        "subset",
        "g60",
        (),
        M60,
        "The plan delivers 9,000 apply starts.",
        "NR",
        "different funnel metric",
    ),
    (
        "sub12",
        "subset",
        "g60",
        (),
        M60,
        "For entry-level roles the plan delivers 10 hires.",
        "MK",
        "segment lowercase",
    ),
    (
        "sub13",
        "subset",
        "g60",
        (),
        M60R,
        "- Projected hires (supervisors only): 5",
        "MK",
        "labelled subset",
    ),
    (
        "sub14",
        "subset",
        "g60",
        (),
        M60R,
        "Projected hires — supervisors: 5",
        "MK",
        "labelled subset dash",
    ),
    (
        "sub15",
        "subset",
        "g60",
        (),
        M60,
        "Total hires (Phase 1): 14",
        "MK",
        "labelled phase subset",
    ),
    (
        "sub16",
        "subset",
        "g60",
        (),
        M60R,
        "Your plan: 5 hires for supervisors.",
        "MK",
        "A3 + role subset",
    ),
    (
        "sub17",
        "subset",
        "g60",
        (),
        M60,
        "The plan delivers 14 full-time hires.",
        "NR",
        "maybe subset",
    ),
    (
        "pct1",
        "pct",
        "g60",
        (),
        M60,
        "With this plan, hires are 20 percent higher than with job boards alone.",
        "U",
        "percent word",
    ),
    (
        "pct2",
        "pct",
        "g60",
        (),
        M60,
        "Applications are 3x what a single board delivers.",
        "U",
        "multiplier",
    ),
    (
        "pct3",
        "pct",
        "g60",
        (),
        M60,
        "Plan for roughly 57 applications to 1 hire.",
        "U",
        "ratio phrased with 'to'",
    ),
    (
        "pct4",
        "pct",
        "g60",
        (),
        M60,
        "This plan expects a 50:1 applications-to-hires ratio.",
        "U",
        "ratio",
    ),
    ("pct5", "pct", "g60", (), M60, "Hires per 100 applications: 2", "U", "rate"),
    (
        "pct6",
        "pct",
        "g60",
        (),
        M60,
        "Applications grow 12 percent month over month under this plan.",
        "U",
        "growth pct",
    ),
    ("pct7", "pct", "g60", (), M60, "Hires: 35 (100% of target).", "U", "engine + pct"),
    (
        "pct8",
        "pct",
        "g60",
        (),
        M60,
        "Expect one hire for roughly every 57 applications.",
        "U",
        "ratio",
    ),
    (
        "dt1",
        "date",
        "g60",
        (),
        M60,
        "By Q3 2026, the plan projects 2,400–7,500 applications.",
        "U",
        "engine + quarter",
    ),
    (
        "dt2",
        "date",
        "g60",
        (),
        M60,
        "Applications: 15 November – 15 December.",
        "U",
        "date range after label",
    ),
    (
        "dt3",
        "date",
        "g60",
        (),
        M60,
        "**Applications open:** 1 Nov 2026; this plan projects 17–35 hires.",
        "U",
        "date + engine",
    ),
    (
        "dt4",
        "date",
        "g60",
        (),
        M60,
        "The plan runs 2026-11-01 to 2027-01-31 and projects 17–35 hires.",
        "U",
        "ISO dates",
    ),
    ("dt5", "date", "g60", (), M60, "Hires: 2026 Q1 onboarding cohort.", "U", "year"),
    (
        "dt6",
        "date",
        "g60",
        (),
        M60,
        "The plan launches 1 March and delivers 14 hires.",
        "R",
        "fabricated with date",
    ),
    (
        "dt7",
        "date",
        "g60",
        (),
        M60,
        "The plan delivers 14 hires in 2027.",
        "FIX",
        "fabricated with year",
    ),
    (
        "dt8",
        "date",
        "g60",
        (),
        M60,
        "Applications close 30 days after launch; this plan projects 17–35 hires.",
        "U",
        "day count",
    ),
    (
        "id1",
        "id",
        "g60",
        (),
        M60,
        "Questions about new hires: 1-800-555-0199.",
        "U",
        "phone number",
    ),
    (
        "id2",
        "id",
        "g60",
        (),
        M60,
        "Requisition 4521 hires a bilingual CSR.",
        "U",
        "req id + verb",
    ),
    (
        "id3",
        "id",
        "g60",
        (),
        M60,
        "Store 214 hires seasonal staff every fall.",
        "U",
        "store id + verb",
    ),
    (
        "id4",
        "id",
        "g60",
        (),
        M60,
        "Version 2 applications form is live on the careers site.",
        "U",
        "version label",
    ),
    (
        "id5",
        "id",
        "g60",
        (),
        M60,
        "Your Indeed employer ID is 77125, and the plan projects 17–35 hires.",
        "U",
        "id + engine",
    ),
    (
        "id6",
        "id",
        "g60",
        (),
        M60,
        "New hires can call ext. 4400 for onboarding; the plan projects 17–35 hires.",
        "U",
        "extension",
    ),
    (
        "ls1",
        "list",
        "g60",
        (),
        M60,
        "1. 900 applications\n2. 14 hires",
        "FIX",
        "bare numbered list of fabricated totals",
    ),
    (
        "ls2",
        "list",
        "g60",
        (),
        M60,
        "Phase 2 hires start in March.",
        "U",
        "phase label",
    ),
    (
        "ls3",
        "list",
        "g60",
        (),
        M60,
        "Round 2 applications reopen in January.",
        "U",
        "round label",
    ),
    (
        "ls4",
        "list",
        "g60",
        (),
        M60,
        "Top 3 applications questions candidates ask:",
        "U",
        "list title",
    ),
    (
        "ls5",
        "list",
        "g60",
        (),
        M60,
        "### Step 3 — Projected hires: 14",
        "FIX",
        "heading label",
    ),
    (
        "ls6",
        "list",
        "g60",
        (),
        M60,
        "2) 35 hires at the plan's best case.",
        "U",
        "engine",
    ),
    (
        "ls7",
        "list",
        "g60",
        (),
        M60,
        "Wave 2 hires onboard in week 6.",
        "U",
        "wave label",
    ),
    (
        "md1",
        "md",
        "g60",
        (),
        M60,
        "This plan delivers **900** applications and **14** hires.",
        "FIX",
        "bold numbers",
    ),
    (
        "md2",
        "md",
        "g60",
        (),
        M60,
        "This plan delivers *900* applications.",
        "FIX",
        "italic number",
    ),
    (
        "md3",
        "md",
        "g60",
        (),
        M60,
        "This plan delivers 900 **applications**.",
        "FIX",
        "bold unit",
    ),
    (
        "md4",
        "md",
        "g60",
        (),
        M60,
        "This plan delivers __14__ hires.",
        "FIX",
        "underscore bold",
    ),
    (
        "md5",
        "md",
        "g60",
        (),
        M60,
        "This plan delivers **2,400–7,500** applications.",
        "U",
        "bold engine",
    ),
    (
        "md6",
        "md",
        "g60",
        (),
        M60,
        "This plan delivers 14 *new* hires.",
        "FIX",
        "italic adjective",
    ),
    ("md7", "md", "g60", (), M60, "This plan delivers 14\xa0hires.", "R", "nbsp"),
    (
        "md8",
        "md",
        "g60",
        (),
        M60,
        "This plan delivers [900 applications](#projection).",
        "FIX",
        "link-wrapped figure",
    ),
    (
        "md9",
        "md",
        "g60",
        (),
        M60,
        "This plan delivers 900¹ applications.",
        "FIX",
        "superscript footnote",
    ),
    (
        "md10",
        "md",
        "g60",
        (),
        M60,
        "This plan delivers ~~900~~ 2,400–7,500 applications.",
        "U",
        "strikethrough + engine",
    ),
    (
        "q1",
        "quote",
        "g60",
        (),
        'Our agency wrote: "This plan delivers 40 hires." Is that right? $60k, 3 months, CSRs in Chicago',
        'No — "This plan delivers 40 hires" is not what Nova projects; this plan projects 17–35 hires.',
        "U",
        "user-pasted quote",
    ),
    (
        "q2",
        "quote",
        "g60",
        (),
        "Agency says: > This plan delivers 40 hires. $60k 3 months CSRs Chicago",
        "> This plan delivers 40 hires.\n\nNova's projection is 17–35 hires.",
        "U",
        "blockquote echo",
    ),
    ("q3", "quote", "g60", (), M60, "```\nprojected_hires = 40\n```", "U", "code"),
    (
        "q4",
        "quote",
        "g60",
        (),
        M60,
        'Ad headline: "Join 500 new hires this fall!"',
        "U",
        "quoted ad copy",
    ),
    (
        "q5",
        "quote",
        "g60",
        (),
        M60,
        "```\nexample\n\nThis plan delivers 900 applications.",
        "FIX",
        "unclosed fence swallows prose",
    ),
    (
        "q6",
        "quote",
        "g60",
        (),
        M60,
        'The plan delivers "about 900 applications" in total.',
        "FIX",
        "scare quotes",
    ),
    (
        "q7",
        "quote",
        "g60",
        (),
        M60,
        "Paste this into your sheet (example values):\n```\n| Metric | Value |\n|---|---|\n| Hires | 40 |\n```",
        "U",
        "table inside code fence",
    ),
    (
        "mp1",
        "multi",
        "g120",
        (M60,),
        "now make it $120k",
        "At $60k the plan projected 17–35 hires; at $120k it projects 35–71 hires.",
        "U",
        "earlier plan true figure",
    ),
    (
        "mp2",
        "multi",
        "g120",
        (M60,),
        "now make it $120k",
        "The $60k plan delivered 17–35 hires and the $120k plan delivers 35–71.",
        "U",
        "earlier plan true figure",
    ),
    (
        "mp3",
        "multi",
        "g120",
        (M60,),
        "now make it $120k",
        "Doubling to $120k takes you from 17–35 hires to 35–71 hires.",
        "U",
        "from-to",
    ),
    (
        "mp4",
        "multi",
        "g60+g120",
        (),
        "compare $60k vs $120k for CSRs in Chicago, 3 months",
        "At $60k: 17–35 hires. At $120k: 35–71 hires.",
        "U",
        "two tools",
    ),
    (
        "mp5",
        "multi",
        "g60+g120",
        (),
        "compare $60k vs $120k for CSRs in Chicago, 3 months",
        "At $60k the plan delivers 40 hires.",
        "FIX",
        "two tools fabricated",
    ),
    (
        "mp6",
        "multi",
        "g60",
        (),
        M60,
        "| Budget | Hires |\n|---|---|\n| $60k | 17–35 |\n| $120k | 35–71 |",
        "U",
        "comparison table only one tool",
    ),
    (
        "mp7",
        "multi",
        "g60",
        ("We need 14 hires on $60k over 3 months for CSRs in Chicago",),
        "actually 20 hires, not 14",
        "Noted: your goal is now 20 hires. This plan projects 17–35 hires.",
        "U",
        "correction",
    ),
    (
        "mp8",
        "multi",
        "g60",
        ("We need 14 hires on $60k over 3 months for CSRs in Chicago",),
        "actually 20 hires, not 14",
        "This plan delivers 14 hires.",
        "MP",
        "stale user number reused as forecast",
    ),
    (
        "mp9",
        "multi",
        "g120",
        (M60,),
        "sorry, it's $120k not $60k",
        "At $120k the plan projects 35–71 hires, up from 17–35.",
        "U",
        "budget correction",
    ),
    (
        "mp10",
        "multi",
        "h50",
        (M60,),
        MH,
        "For the nurse plan, this plan projects 14 hires.",
        "FIX",
        "switched brief fabricated",
    ),
    (
        "co1",
        "coincide",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago; we have 35 stores",
        "This plan delivers 35 hires.",
        "U",
        "engine == user",
    ),
    (
        "co2",
        "coincide",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago; we get about 900 applications a month organically",
        "This plan delivers 900 applications.",
        "MP",
        "user organic number as forecast",
    ),
    (
        "co3",
        "coincide",
        "g60",
        (),
        "make a $60k, 3-month media plan for customer service reps in Chicago; our team of 14 recruiters",
        "This plan delivers 14 hires.",
        "MP",
        "team size as forecast",
    ),
    (
        "co4",
        "coincide",
        "g60",
        (),
        M60,
        "The industry CPA benchmark is $8–$25, and this plan's CPA is $8–$25.",
        "U",
        "engine==benchmark",
    ),
    (
        "co5",
        "coincide",
        "g60",
        (),
        M60,
        "This plan delivers 14 hires.",
        "R",
        "fabricated == a channel figure",
    ),
    (
        "co6",
        "coincide",
        "g60",
        (),
        M60,
        "Global boards deliver 14 hires.",
        "U",
        "true channel",
    ),
    (
        "co7",
        "coincide",
        "g60",
        (),
        M60,
        "Job boards deliver about 14 hires.",
        "U",
        "true channel, uncued wording",
    ),
    (
        "co8",
        "coincide",
        "g60",
        (),
        M60,
        "Programmatic and global boards together bring 26 hires.",
        "NR",
        "sum of channels",
    ),
    (
        "co9",
        "coincide",
        "g60",
        (),
        "We need a hundred hires, budget $60k over 6 months, CSRs in Chicago",
        "Your plan: 100 hires over 6 months on $60k.",
        "U",
        "restated brief, 'a hundred'",
    ),
    (
        "co10",
        "coincide",
        "g60",
        (),
        "We need two-hundred hires, budget $60k, CSRs in Chicago",
        "Your plan: 200 hires on $60k — the engine projects 17–35.",
        "U",
        "restated brief, hyphenated words",
    ),
    (
        "co11",
        "coincide",
        "g60",
        (),
        "We need two dozen hires, budget $60k, CSRs in Chicago",
        "Your goal of 24 hires sits inside this plan's 17–35 range.",
        "U",
        "two dozen",
    ),
    (
        "rt1",
        "rate",
        "g60",
        (),
        M60,
        "This plan delivers 40 hires a month.",
        "FIX",
        "inflated monthly rate",
    ),
    (
        "rt2",
        "rate",
        "g60",
        (),
        M60,
        "This plan delivers about 6 hires per month.",
        "U",
        "plausible rate",
    ),
    (
        "rt3",
        "rate",
        "g60",
        (),
        M60,
        "Monthly, the plan delivers 40 hires.",
        "FIX",
        "monthly adverb",
    ),
    (
        "rt4",
        "rate",
        "g60",
        (),
        M60,
        "That's roughly 1,000 applications every week from this plan.",
        "FIX",
        "inflated weekly",
    ),
    (
        "rt5",
        "rate",
        "g60",
        (),
        M60,
        "The plan delivers 3 hires weekly.",
        "FIX",
        "inflated weekly",
    ),
    (
        "rt6",
        "rate",
        "g60",
        (),
        M60,
        TABLE_HDR + "| Total monthly hires | 6 |",
        "NR",
        "total of a rate",
    ),
    (
        "cs1",
        "cost",
        "g60",
        (),
        M60,
        "This plan's cost per applicant is $66.67.",
        "FIX",
        "applicant",
    ),
    (
        "cs2",
        "cost",
        "g60",
        (),
        M60,
        "This plan's cost-per-apply is $66.67.",
        "FIX",
        "cost-per-apply",
    ),
    (
        "cs3",
        "cost",
        "g60",
        (),
        M60,
        "This plan's per-application cost is $66.67.",
        "FIX",
        "per-application cost",
    ),
    (
        "cs4",
        "cost",
        "g60",
        (),
        M60,
        "This plan's cost per click is $0.56.",
        "U",
        "CPC true",
    ),
    (
        "cs5",
        "cost",
        "g60",
        (),
        M60,
        "Each hire will cost about $4,000 under this plan.",
        "FIX",
        "each hire will cost",
    ),
    (
        "cs6",
        "cost",
        "g60",
        (),
        M60,
        "This plan works out to $4,000 a hire.",
        "FIX",
        "a hire",
    ),
    (
        "cs7",
        "cost",
        "g60",
        (),
        M60,
        "This plan's cost to hire is $4,000.",
        "FIX",
        "cost to hire",
    ),
    (
        "cs8",
        "cost",
        "g60",
        (),
        M60,
        "The plan's CPA (cost per acquisition) is $66.67.",
        "FIX",
        "CPA parenthetical",
    ),
    (
        "cs9",
        "cost",
        "g60",
        (),
        M60,
        "Expect a cost per application of $8–$25 and a CPC of $0.56.",
        "U",
        "engine + cpc",
    ),
    ("cs10", "cost", "g60", (), M60, "CPH: $1,714", "U", "engine point"),
    (
        "cs11",
        "cost",
        "g60",
        (),
        M60,
        "This plan's cost per new hire is $4,000.",
        "FIX",
        "per new hire",
    ),
    (
        "ne1",
        "lang",
        "g60",
        (),
        M60,
        "Este plan genera 900 solicitudes y 14 contrataciones.",
        "FIX",
        "spanish",
    ),
    (
        "ne2",
        "lang",
        "g60",
        (),
        M60,
        "Der Plan bringt 900 Bewerbungen und 14 Einstellungen.",
        "FIX",
        "german",
    ),
    (
        "ne3",
        "lang",
        "g60",
        (),
        M60,
        "Ce plan génère 900 candidatures.",
        "FIX",
        "french",
    ),
    ("ne4", "lang", "g60", (), M60, "El plan genera 900 applications.", "FIX", "mixed"),
    (
        "ne5",
        "lang",
        "g60",
        (),
        M60,
        "Este plan genera 2.400–7.500 solicitudes.",
        "U",
        "spanish engine",
    ),
    (
        "uc1",
        "unicode",
        "g60",
        (),
        M60,
        "This plan projects 17−35 hires.",
        "U",
        "minus sign engine",
    ),
    (
        "uc2",
        "unicode",
        "g60",
        (),
        M60,
        "This plan projects 17‒35 hires.",
        "U",
        "figure dash engine",
    ),
    (
        "uc3",
        "unicode",
        "g60",
        (),
        M60,
        "This plan delivers 9−14 hires.",
        "R",
        "minus sign fabricated range",
    ),
    (
        "uc4",
        "unicode",
        "g60",
        (),
        M60,
        "This plan delivers 900‑1,200 applications.",
        "R",
        "nb-hyphen fabricated range",
    ),
    (
        "uc5",
        "unicode",
        "g60",
        (),
        M60,
        "This plan delivers 900 ‐ 1,200 applications.",
        "R",
        "hyphen U+2010 fabricated",
    ),
    (
        "uc6",
        "unicode",
        "g60",
        (),
        M60,
        "Your plan's CPA is $40−$60.",
        "FIX",
        "minus money range",
    ),
    (
        "uc7",
        "unicode",
        "g60",
        (),
        M60,
        "This plan delivers ９００ applications.",
        "R",
        "fullwidth digits",
    ),
    (
        "uc8",
        "unicode",
        "g60",
        (),
        M60,
        "This plan projects 17 – 35 hires.",
        "U",
        "spaced en dash engine",
    ),
    (
        "dg1",
        "dagger",
        "g60",
        (),
        M60,
        "This plan delivers 900† applications.",
        "U",
        "pre-daggered by model",
    ),
    (
        "dg2",
        "dagger",
        "g60",
        (),
        M60,
        "This plan delivers 900 applications†.",
        "FIX",
        "dagger after unit: no double mark",
    ),
    (
        "dg3",
        "dagger",
        "g60",
        (),
        M60,
        "This plan projects 2,400–7,500 applications.\n\n† estimate not traced to Nova's planning engine",
        "U",
        "stray legend",
    ),
    (
        "te1",
        "toolerr",
        "none",
        (),
        M60,
        "This plan delivers 900 applications.",
        "FIX",
        "None tool result",
    ),
    (
        "te2",
        "toolerr",
        "err",
        (),
        M60,
        "This plan delivers 900 applications.",
        "FIX",
        "error tool result",
    ),
    (
        "te3",
        "toolerr",
        "garbage",
        (),
        M60,
        "This plan delivers 900 applications.",
        "FIX",
        "non-json tool result",
    ),
    (
        "te4",
        "toolerr",
        "emptytext",
        (),
        M60,
        "This plan delivers 900 applications.",
        "FIX",
        "projection with empty text",
    ),
    (
        "te5",
        "toolerr",
        "nulltext",
        (),
        M60,
        "This plan delivers 900 applications.",
        "FIX",
        "projection with null values",
    ),
    (
        "te6",
        "toolerr",
        "",
        (),
        "how many hires will this get?",
        "This plan delivers 900 applications and 14 hires.",
        "FIX",
        "no tool, no budget in msg (follow-up)",
    ),
    (
        "te7",
        "toolerr",
        "",
        (),
        M60,
        "This plan delivers 900 applications.",
        "FIX",
        "no tool, budget in msg",
    ),
    (
        "te8",
        "toolerr",
        "g60dup",
        (),
        M60,
        "This plan delivers 900 applications.",
        "R",
        "same projection twice",
    ),
    (
        "ps1",
        "shift",
        "g60",
        (),
        M60,
        "The plan delivers 14 hires (9 from Indeed) and 900 applications.",
        "FIX",
        "rewrite+mark positions",
    ),
    (
        "ps2",
        "shift",
        "g60",
        (),
        M60,
        "This plan delivers 14 hires, 900 applications and a $66.67 CPA.",
        "R",
        "three rewrites",
    ),
    (
        "ps3",
        "shift",
        "g60",
        (),
        M60,
        "Your plan: 14 hires. Indeed alone: 9 hires. Total hires: 14.",
        "FIX",
        "mix",
    ),
    (
        "tb1",
        "table",
        "g60",
        (),
        M60,
        "| Channel | Hires |\n|---|---|\n| Indeed | 5 |\n| Total (Chicago only) | 14 |",
        "MK",
        "total row of a subset",
    ),
    (
        "tb2",
        "table",
        "g60",
        (),
        M60,
        "| First 2 months | Hires |\n|---|---|\n| Nov | 5 |\n| Dec | 6 |\n| Total | 11 |",
        "MK",
        "total row of a time slice",
    ),
    (
        "tb3",
        "table",
        "g60",
        (),
        M60,
        "  | Metric | Value |\n  |---|---|\n  | Hires | 17–35 |",
        "U",
        "indented engine table",
    ),
    (
        "tb4",
        "table",
        "g60",
        (),
        M60,
        TABLE_HDR + "| Hires | 17–35",
        "U",
        "row missing trailing pipe",
    ),
    (
        "tb5",
        "table",
        "g60",
        (),
        M60,
        "| Scenario | Budget | Hires |\n|---|---|---|\n| Current | $60k | 17–35 |\n| Doubled | $120k | 70 |",
        "NR",
        "scenario table",
    ),
    (
        "tb6",
        "table",
        "g60",
        (),
        M60,
        "| Channel | Applications | CPA |\n|---|---|---|\n| Total | 7,247 | $8.28 |",
        "U",
        "engine points total row",
    ),
    (
        "tb7",
        "table",
        "g60",
        (),
        M60,
        "| Channel | Applications |\n|---|---|\n| Total excl. employer branding | 7,167 |",
        "NR",
        "total excluding a channel",
    ),
    (
        "hy1",
        "hyphen",
        "g60",
        (),
        M60,
        "This plan projects 17‑35 hires.",
        "U",
        "true range, U+2011",
    ),
    (
        "hy2",
        "hyphen",
        "g60",
        (),
        M60,
        "This plan projects 2,400‑7,500 applications.",
        "U",
        "true range, U+2011",
    ),
    (
        "hy3",
        "hyphen",
        "g60",
        (),
        M60,
        "This plan's CPA is $8‑$25.",
        "U",
        "true money range, U+2011",
    ),
    (
        "hy4",
        "hyphen",
        "g60",
        (),
        M60,
        "This plan's cost per hire is $1,675‑$3,350.",
        "U",
        "true money range, U+2011",
    ),
    (
        "hy5",
        "hyphen",
        "g60",
        (),
        M60,
        "Expect 17‐35 hires from this plan.",
        "U",
        "true range, U+2010",
    ),
    (
        "hy6",
        "hyphen",
        "g60",
        (),
        M60,
        "Projected hires: 17−35",
        "U",
        "true range, minus, label",
    ),
    (
        "hy7",
        "hyphen",
        "g60",
        (),
        M60,
        TABLE_HDR + "| Hires | 17‑35 |",
        "U",
        "true range in table",
    ),
    (
        "hy8",
        "hyphen",
        "g60",
        (),
        M60,
        "| Channel | Hires |\n|---|---|\n| Total | 17‑35 |",
        "U",
        "true range in total row",
    ),
    (
        "w4",
        "subset2",
        "g60",
        (),
        M60,
        "For the remaining 3 months, the plan delivers 8 hires.",
        "MK",
        "remaining months",
    ),
    (
        "w7",
        "subset2",
        "g60",
        (),
        M60,
        "At the current pace, the plan delivers 10 hires.",
        "NR",
        "pace",
    ),
    (
        "w8",
        "subset2",
        "g60",
        (),
        M60,
        "With a 6-month runway, the plan delivers 40 hires.",
        "NR",
        "other duration",
    ),
    (
        "w11",
        "subset2",
        "g60",
        (),
        M60,
        "Minus employer branding, the plan delivers 7,167 applications.",
        "U",
        "true derived figure",
    ),
    (
        "w13",
        "subset2",
        "g60",
        (),
        M60,
        "The plan delivers 14 hires to your Dallas team.",
        "MK",
        "team subset",
    ),
    (
        "w21",
        "subset2",
        "g60",
        (),
        M60,
        "Your plan: 14 hires on half the budget.",
        "NR",
        "half budget",
    ),
]
assert len(CASES) == 175 and len({c[0] for c in CASES}) == 175

# The second round's 62 fresh cases (id, category, brief, turns, message, answer,
# code, note).
FRESH_CASES = [
    (
        "T1",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Total | 900 | 14 |",
        "R",
        "exact Total",
    ),
    (
        "T2",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Totals | 900 | 14 |",
        "NRW",
        "Totals",
    ),
    (
        "T3",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| TOTAL | 900 | 14 |",
        "ANY",
        "TOTAL caps",
    ),
    (
        "T4",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Total: | 900 | 14 |",
        "ANY",
        "Total colon",
    ),
    (
        "T5",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Overall | 900 | 14 |",
        "NRW",
        "Overall",
    ),
    (
        "T6",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Total (all roles) | 900 | 14 |",
        "NRW",
        "qualifier all roles",
    ),
    (
        "T7",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Total — Chicago | 900 | 14 |",
        "NRW",
        "dash qualifier",
    ),
    (
        "T8",
        "total",
        "g60",
        (),
        M60,
        "| Source | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Total | 900 | 14 |",
        "NRW",
        "non-Channel header",
    ),
    (
        "T9",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Monthly hires |\n|---|---|\n| Indeed | 4 |\n| Total | 6 |",
        "NRW",
        "monthly column",
    ),
    (
        "T10",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Hires (Chicago) |\n|---|---|\n| Indeed | 4 |\n| Total | 10 |",
        "NRW",
        "subset column",
    ),
    (
        "T11",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Q1 hires |\n|---|---|\n| Indeed | 4 |\n| Total | 8 |",
        "NRW",
        "quarter column",
    ),
    (
        "T12",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Supervisor hires |\n|---|---|\n| Indeed | 4 |\n| Total | 5 |",
        "NRW",
        "role column",
    ),
    (
        "T13",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Qualified applications |\n|---|---|\n| Indeed | 50 |\n| Total | 600 |",
        "NRW",
        "qualified column",
    ),
    (
        "T14",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Incremental hires |\n|---|---|\n| Indeed | 4 |\n| Total | 10 |",
        "NRW",
        "incremental column",
    ),
    (
        "T15",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Hires |\n|---|---|\n| Indeed | 4 |\n| Total | 14 |\n\n### If you double the budget\n\n| Channel | Hires |\n|---|---|\n| Indeed | 4 |\n| Total | 70 |",
        "NRW",
        "second table is a scenario (first Total R)",
    ),
    (
        "T16",
        "total",
        "g60",
        (),
        M60,
        "### Chicago only\n\n| Channel | Hires |\n|---|---|\n| Indeed | 4 |\n| Total | 10 |",
        "NRW",
        "heading scopes table",
    ),
    (
        "T17",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Total | 2,400–7,500 | 17–35 |",
        "U",
        "engine ranges",
    ),
    (
        "T18",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Total | 1,000–2,000 | 10–20 |",
        "R",
        "fabricated ranges",
    ),
    (
        "T19",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Total | 7,247 | 35 |",
        "U",
        "engine points",
    ),
    (
        "T20",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Total | 7\u202f247 | 35 |",
        "U",
        "narrow nbsp thousands",
    ),
    (
        "T21",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Total | ~900 | ~14 |",
        "R",
        "tilde",
    ),
    (
        "T22",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Total | 900 (est.) | 14 (est.) |",
        "ANY",
        "annotated cells",
    ),
    (
        "T23",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Spend | CPA |\n|---|---|---|\n| Indeed | $18,037 | $7.50 |\n| Total | $60,000 | $9.10 |",
        "R",
        "CPA total",
    ),
    (
        "T24",
        "total",
        "g60",
        (),
        M60 + "; we expect 900 applications",
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Total | 900 | 2,400 |",
        "ANY",
        "user number in Total (apps) + wrong-kind cell",
    ),
    (
        "T25",
        "total",
        "g60+g120",
        (),
        "compare $60k vs $120k for CSRs in Chicago, 3 months",
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Total | 900 | 14 |",
        "NRW",
        "two projections",
    ),
    (
        "T26",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Budget | Hires |\n|---|---|---|\n| Indeed | $18,037 | 4 |\n| Total | $60,000 | 14 |",
        "R",
        "budget column untouched, hires rewritten",
    ),
    (
        "T27",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| **Total** | **900** | **14** |",
        "R",
        "bold",
    ),
    (
        "T28",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Hires/month |\n|---|---|\n| Indeed | 4 |\n| Total | 6 |",
        "ANY",
        "ratio column",
    ),
    (
        "T29",
        "total",
        "g60",
        (),
        M60,
        "| Channels | Hires |\n|---|---|\n| Indeed | 4 |\n| Total | 14 |",
        "R",
        "plural header",
    ),
    (
        "T30",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Hires |\n|---|---|\n| Indeed | 4 |\n| Total | 35 |",
        "U",
        "engine high end",
    ),
    (
        "T31",
        "total",
        "point",
        (),
        M60,
        "| Channel | Hires |\n|---|---|\n| Indeed | 4 |\n| Total | 14 |",
        "R",
        "tool total is a point",
    ),
    (
        "T32",
        "total",
        "g60",
        (),
        M60,
        "> | Channel | Hires |\n> |---|---|\n> | Total | 14 |",
        "ANY",
        "table in blockquote",
    ),
    (
        "T33",
        "total",
        "g60",
        (),
        M60,
        "| **Channel** | **Hires** |\n|---|---|\n| Indeed | 4 |\n| Total | 14 |",
        "R",
        "bold header",
    ),
    (
        "T34",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Hires (worst case) |\n|---|---|\n| Indeed | 4 |\n| Total | 20 |",
        "NRW",
        "bound column",
    ),
    (
        "T35",
        "total",
        "g60",
        (),
        M60,
        "| Channel | Applications | Hires |\n|---|---|---|\n| Indeed | 503 | 4 |\n| Total (est.) | 900 | 14 |",
        "NRW",
        "Total (est.)",
    ),
    ("P1", "prose", "g60", (), M60, "**Total hires:** 14", "FIX", "bold label"),
    ("P2", "prose", "g60", (), M60, "Total: 14 hires.", "FIX", "total colon prose"),
    ("P3", "prose", "g60", (), M60, "Projected hires — 14", "FIX", "dash label"),
    (
        "P4",
        "prose",
        "g60",
        (),
        M60,
        "This plan delivers 14 hires (Indeed 9).",
        "FIX",
        "parenthetical channel",
    ),
    (
        "P5",
        "prose",
        "g60",
        (),
        M60,
        "This plan delivers 14 hires, mostly from programmatic.",
        "FIX",
        "channel word after a total",
    ),
    (
        "P6",
        "prose",
        "g60",
        (),
        M60,
        "This plan delivers 12 hires, led by programmatic.",
        "FIX",
        "channel word, value == programmatic's",
    ),
    (
        "P7",
        "prose",
        "g60",
        (),
        M60,
        "Across all channels the plan delivers 8 hires.",
        "FIX",
        "all channels",
    ),
    (
        "P8",
        "prose",
        "g60",
        (),
        M60,
        "The plan delivers 1,645 applications from regional boards.",
        "U",
        "true channel",
    ),
    (
        "P9",
        "prose",
        "g60",
        (),
        M60,
        "Regional boards: 1,645 applications; the plan overall: 2,400–7,500 applications.",
        "U",
        "true channel + engine",
    ),
    (
        "P10",
        "prose",
        "g60",
        (),
        M60 + "; we need 40 hires",
        "| Channel | Hires |\n|---|---|\n| Indeed | 4 |\n| Total | 40 |",
        "MP",
        "user goal in Total row",
    ),
    (
        "P11",
        "prose",
        "g60",
        (),
        M60 + "; we need forty hires",
        "Your goal of 40 hires is above this plan's 17–35.",
        "U",
        "word number echoed as digits",
    ),
    (
        "P12",
        "prose",
        "g60",
        (),
        M60,
        "```\nThis plan delivers 900 applications\n```",
        "U",
        "code",
    ),
    (
        "P13",
        "prose",
        "g60",
        (),
        M60,
        "This plan delivers 14† hires.",
        "U",
        "already marked",
    ),
    (
        "P14",
        "prose",
        "g60",
        (),
        M60,
        "Expect 2,400‑7,500 applications.",
        "U",
        "U+2011 engine",
    ),
    ("P15", "prose", "g60", (), M60, "Expect 17 – 35 hires.", "U", "spaced en dash"),
    ("P16", "prose", "g60", (), M60, "Expect 17—35 hires.", "U", "em dash"),
    ("P17", "prose", "g60", (), M60, "Expect 17 to 35 hires.", "U", "to"),
    ("P18", "prose", "g60", (), M60, "Expect between 17 and 35 hires.", "U", "between"),
    (
        "P19",
        "prose",
        "g60",
        (),
        M60,
        "Expect 17-35 hires (best case 35).",
        "U",
        "hyphen + paren",
    ),
    (
        "P20",
        "prose",
        "g60",
        (),
        M60,
        "The plan's CPA is $8.00–$25.00.",
        "U",
        "decimals",
    ),
    (
        "P21",
        "prose",
        "g60",
        (),
        M60,
        "CPA: $8 – $25 (industry $8–$25).",
        "U",
        "spaced money",
    ),
    (
        "P22",
        "prose",
        "g60",
        (),
        M60,
        "Cost per hire: $1,675 to $3,350.",
        "U",
        "to money",
    ),
    ("P23", "prose", "g60", (), M60, "Cost per hire: $1.7k–$3.4k.", "U", "k money"),
    (
        "P24",
        "prose",
        "g60",
        (),
        M60,
        "Cost per hire: USD 1,675–3,350.",
        "U",
        "USD prefix",
    ),
    (
        "P25",
        "prose",
        "g60",
        (),
        M60,
        "The plan projects 17–35 hires, or about 3–6 a month.",
        "U",
        "rate restatement",
    ),
    (
        "P26",
        "prose",
        "g60",
        (),
        M60,
        "That's about 10 hires a month.",
        "U",
        "rate within 3-month spread",
    ),
    (
        "P27",
        "prose",
        "g60",
        (),
        M60,
        "That's about 20 hires a month.",
        "FIX",
        "inflated rate",
    ),
]
assert len(FRESH_CASES) == 62 and not {c[0] for c in FRESH_CASES} & {
    c[0] for c in CASES
}

# Every remaining FALSE MARK (a dagger on a figure the verifier called true or not a
# plan figure), across all corpora, with the reason it stays. Prose is mark-only,
# so each costs a dagger, never a changed number.
ACCEPTED_F: Dict[str, str] = {
    "v3:id2": "a label + number + unit word ('Requisition 4521 hires') reads as a count; telling labels apart needs a word list",
    "v3:id3": "label + number ('Store 214 hires'), as id2",
    "v3:id4": "label + number ('Version 2 applications'), as id2",
    "v3:ls2": "label + number ('Phase 2 hires'), as id2",
    "v3:ls3": "label + number ('Round 2 applications'), as id2",
    "v3:ls4": "label + number ('Top 3 applications'), as id2",
    "v3:ls7": "label + number ('Wave 2 hires'), as id2",
    "v3:dt2": "a day-of-month after 'Applications:' ('15 November') reads as a count; a date parser is a word list",
    "v3:mp1": "an EARLIER turn's true range; this turn's tool results hold only the new projection",
    "v3:mp2": "an earlier turn's true range, as mp1",
    "v3:mp3": "an earlier turn's true range, as mp1",
    "v3:mp6": "a $120k row in a turn whose only projection is $60k: nothing to trace it to",
    "v3:co7": "a true channel figure in words outside the channel cue ('Job boards'); no new cue words",
    "v3:w11": "a figure derived by arithmetic from the tool (7,247 - 80) is not itself in the result",
    "f:T17": "the Total row (the engine's ranges) is untouched; the dagger is on the made-up '| Indeed | 503 | 4 |' row, which no tool computed",
    "f:T19": "as T17 (Total row of engine points untouched; the made-up Indeed row is marked)",
    "f:T20": "as T17 (narrow-space thousands in the Total row read correctly; the Indeed row is marked)",
    "f:T30": "as T17 (Total 35 is the engine's high end, untouched; the Indeed row is marked)",
    "c1:own2_conditional_cpa": "an untraced conditional CPA ('could be $45 if ...'); mood is not read (expectation changed from unchanged to marked)",
}
# Every remaining MISS (a wrong figure left unmarked), with the reason it stays.
ACCEPTED_M: Dict[str, str] = {
    "v3:md8": "a figure inside a markdown link's text is protected (link text is a source title)",
    "v3:q5": "an unclosed code fence protects everything after it (code is never judged)",
    "v3:q6": "quoted copy is protected (ad copy, a pasted agency claim)",
    "v3:ne1": "NOT COVERED: Spanish answer, no unit word recognised",
    "v3:ne2": "NOT COVERED: German answer, no unit word recognised",
    "v3:ne3": "NOT COVERED: French answer, no unit word recognised",
    "v3:te6": "no tool, no budget in ANY turn of this case: nothing says it is a plan chat (with an earlier budget turn it is marked: test_a_no_tool_follow_up_in_a_budget_conversation_is_marked)",
    "c2:c9": "no tool, no budget anywhere (the corpus's known partial)",
    "f:P4": "KNOWN LIMIT: a channel word in the clause ('(Indeed 9)') excuses a figure equal to a channel's own (14 = global boards)",
    "f:P5": "KNOWN LIMIT: channel word ('programmatic') in the clause, as P4",
    "f:P6": "KNOWN LIMIT: channel word, and 12 IS programmatic's own figure",
    "f:P27": "KNOWN LIMIT: a rate is checked only when its clause names the plan",
    "c2:a12": "'about 7,500 applications' IS the engine's high end: it traces, so no mark; the basis line names it the best case (expectation changed from R to OK)",
}
# Expectations changed for a reason other than "prose is now mark-only": the
# scoreboard counts them against the ORIGINAL expectation.
ORIGINAL_EXPECTATION = {"c1:own2_conditional_cpa": "U", "c2:a12": "R"}

_APPENDIX = (
    "\n\n*Projection basis:",
    "\n\n**Projected outcomes (Nova planning engine)**",
)


def _split(out: str) -> Tuple[str, str]:
    cuts = [out.find(m) for m in _APPENDIX if out.find(m) >= 0]
    cut = min(cuts) if cuts else len(out)
    return out[:cut], out[cut:]


def _context(msg: str, text: str) -> Tuple[bool, bool]:
    """plan_context / plan_shaped exactly as Nova computes them."""
    qt = nova._classify_query_type(msg)
    pc = is_plan_shaped(qt, text) or (parse_budget(msg) or 0) >= 1000
    ps = bool(_CHANNEL_TABLE_RE.search(text)) or (
        qt == "media_plan" and len(text) >= 600
    )
    return pc, ps


def _enforce(case, text=None):
    _cid, _cat, brief, turns, msg, body, _exp, _note = case
    body = body if text is None else text
    pc, ps = _context(msg, body)
    return pf.enforce(
        body,
        BRIEFS[brief],
        msg,
        plan_context=pc,
        plan_shaped=ps,
        user_turns=list(turns) + [msg],
    )


_DAGGER_IN_TOKEN = re.compile(r"\d†[\d\-‐‑‒–—−]|[‐‑‒–—−-]\d+†[‐‑‒–—−-]\d")


def _assert_not_mangled(cid: str, text: str, out: str, stats: Dict[str, int]):
    """K: outside a rewrite the only change is a dagger; never a dagger inside a
    number; and a second pass changes nothing."""
    core = _split(out)[0]
    if not stats["rewritten"]:
        assert core.replace(pf.DAGGER, "") == text.replace(
            pf.DAGGER, ""
        ), f"{cid}: changed beyond a dagger: {core!r}"
    assert not _DAGGER_IN_TOKEN.search(out), f"{cid}: dagger inside a number: {out!r}"


@pytest.mark.parametrize("case", CASES, ids=[c[0] for c in CASES])
def test_verifier3_case(case):
    cid, _cat, _brief, _turns, _msg, text, exp, note = case
    out, stats = _enforce(case)
    rewritten, marked = stats["rewritten"] > 0, stats["marked"] > 0
    assert not rewritten, f"{cid}: WRONGFUL REWRITE ({note}): {out!r}"
    _assert_not_mangled(cid, text, out, stats)
    key = f"v3:{cid}"
    if exp == "U":
        assert marked == (key in ACCEPTED_F), f"{cid} ({note}): {out!r}"
    elif exp in ("R", "FIX", "MK"):
        assert marked != (key in ACCEPTED_M), f"{cid} ({note}): {out!r}"
    elif exp == "MP":
        assert not marked, f"{cid}: the user's own number is theirs: {out!r}"
    else:
        assert exp == "NR"
    again, stats2 = _enforce(case, out)
    assert again == out and stats2["rewritten"] == 0, f"{cid}: not idempotent"


@pytest.mark.parametrize("case", FRESH_CASES, ids=[c[0] for c in FRESH_CASES])
def test_fresh_case(case):
    """The second round's cases: nothing is ever rewritten (its 9 wrongful
    Total-row rewrites were W); a wrong exact Total row (code R) is marked."""
    cid, _cat, _brief, _turns, _msg, text, exp, note = case
    out, stats = _enforce(case)
    marked = stats["marked"] > 0
    assert stats["rewritten"] == 0, f"{cid}: REWRITE ({note}): {out!r}"
    _assert_not_mangled(cid, text, out, stats)
    key = f"f:{cid}"
    if exp == "U":
        assert marked == (key in ACCEPTED_F), f"{cid} ({note}): {out!r}"
    elif exp in ("R", "FIX"):
        assert marked != (key in ACCEPTED_M), f"{cid} ({note}): {out!r}"
    elif exp == "MP":
        for typed in re.findall(r"\d{2,}", _msg):
            assert f"{typed}†" not in out, f"{cid}: the user's {typed} was marked"
    else:
        assert exp in ("NRW", "ANY")
    again, _ = _enforce(case, out)
    assert again == out, f"{cid}: not idempotent"


def test_total_rows_of_other_measures_are_marked_never_rewritten():
    """The round-2 W cases: a Total row of a monthly / qualified / worst-case /
    quarter / role / incremental / city column, or of a table under a scenario
    or a city heading, keeps its number and gains a dagger."""
    by_id = {c[0]: c for c in FRESH_CASES}
    for cid, total in (
        ("T9", "6"), ("T10", "10"), ("T11", "8"), ("T12", "5"), ("T13", "600"),
        ("T14", "10"), ("T16", "10"), ("T34", "20"),
    ):  # fmt: skip
        out, st = _enforce(by_id[cid])
        assert f"| Total | {total}\u2020 |" in out and st["rewritten"] == 0, cid
    out, _ = _enforce(by_id["T15"])
    assert "| Total | 14\u2020 |" in out and "| Total | 70\u2020 |" in out


@pytest.fixture(scope="module")
def engine():
    return nova.Nova()


@pytest.fixture(autouse=True)
def _flags(monkeypatch):
    monkeypatch.setenv("NOVA_NUMBER_VERIFIER_ENABLED", "1")
    monkeypatch.setenv("NOVA_CITATIONS_ENABLED", "0")
    monkeypatch.setenv("NOVA_PLAN_FIGURE_GUARD_ENABLED", "1")


def _pipeline(engine_, case, text=None):
    _cid, _cat, brief, turns, msg, body, _exp, _note = case
    nova._set_turn_tool_capture(
        nova._new_turn_state(
            msg, [{"role": "user", "content": t} for t in turns], session_id=""
        )
    )
    try:
        return engine_._apply_t3_postprocessing(
            {
                "response": body if text is None else text,
                "sources": [],
                "confidence": 0.8,
                "tools_used": ["query_budget_projection"] if brief else [],
                "_tool_results_raw": list(BRIEFS[brief]),
            },
            msg,
        )["response"]
    finally:
        nova._set_turn_tool_capture(None)


@pytest.mark.parametrize("case", CASES, ids=[c[0] for c in CASES])
def test_verifier3_case_through_the_pipeline_keeps_one_legend(engine, case):
    """What the reader gets: one legend whenever a dagger is shown, never two, and
    and never a "corrected" line (nothing is ever rewritten)."""
    out = _pipeline(engine, case)
    legend = out.count(nova._T3_UNVERIFIED_FOOTER)
    if pf.DAGGER in out.replace(nova._T3_UNVERIFIED_FOOTER, ""):
        assert legend == 1, out
    assert legend <= 1 and "corrected" not in out.lower()


# ---------------------------------------------------------------------------
# The scoreboard over every corpus
# ---------------------------------------------------------------------------
def _all_corpora() -> List[Tuple[str, str, str, list, str, list, bool, bool]]:
    """(key, expectation, text, turns, message, tool results, plan_context,
    plan_shaped) for every case of the three corpora."""
    from tests import test_nova_plan_guard_corpus as c1
    from tests import test_nova_plan_guard_corpus2 as c2

    out = []
    kind_code = {"unchanged": "U", "marked": "MK", "rewritten": "R"}
    for c in c1.CASES:
        out.append(
            (f"c1:{c.id}", kind_code[c.kind], c.text, [], c.message,
             c1._raws(c), c.plan_context, False)
        )  # fmt: skip
    for cid, _cat, brief, turns, msg, text, exp in c2.ALL_CASES:
        out.append(
            (f"c2:{cid}", exp, text, list(turns), msg, c2._raws(brief),
             c2._plan_context(brief, msg), False)
        )  # fmt: skip
    for cid, _cat, brief, turns, msg, text, exp, _note in CASES:
        pc, ps = _context(msg, text)
        out.append((f"v3:{cid}", exp, text, list(turns), msg, BRIEFS[brief], pc, ps))
    for cid, _cat, brief, turns, msg, text, exp, _note in FRESH_CASES:
        pc, ps = _context(msg, text)
        out.append((f"f:{cid}", exp, text, list(turns), msg, BRIEFS[brief], pc, ps))
    return out


def test_the_scoreboard_over_every_corpus():
    """W = 0 (no rewrite at all) and K = 0 across all 635 cases; F and M are
    exactly the listed, justified cases (round 3 on the old guard: W 43, K 13,
    F 20, M 32; round 2 on the Total-row rewrite: W 9)."""
    score: Dict[str, List[str]] = {"W": [], "K": [], "F": [], "M": []}
    corpora = _all_corpora()
    assert len(corpora) == 635
    for key, exp, text, turns, msg, tools, pc, ps in corpora:
        exp = ORIGINAL_EXPECTATION.get(key, exp)
        t0 = time.perf_counter()
        out, st = pf.enforce(
            text, tools, msg, plan_context=pc, plan_shaped=ps, user_turns=turns + [msg]
        )
        slow = time.perf_counter() - t0 > 0.5
        rew, mk = st["rewritten"] > 0, st["marked"] > 0
        if rew:
            score["W"].append(key)
        core = _split(out)[0]
        again, _ = pf.enforce(
            out, tools, msg, plan_context=pc, plan_shaped=ps, user_turns=turns + [msg]
        )
        if (
            slow
            or again != out
            or _DAGGER_IN_TOKEN.search(out)
            or core.replace(pf.DAGGER, "") != text.replace(pf.DAGGER, "")
        ):
            score["K"].append(key)
        if exp in ("U", "OK", "LEFT") and mk:
            score["F"].append(key)
        if exp in ("R", "FIX", "MK") and not mk:
            score["M"].append(key)
    assert score["W"] == [] and score["K"] == []
    assert sorted(score["F"]) == sorted(ACCEPTED_F) and len(score["F"]) <= 20
    assert sorted(score["M"]) == sorted(ACCEPTED_M) and len(score["M"]) < 32


# ---------------------------------------------------------------------------
# Generative: nothing is ever replaced, in prose or in any table row
# ---------------------------------------------------------------------------
_QUALIFIERS = (
    "",
    "For supervisors,",
    "Through job boards",
    "With twice the budget,",
    "With €200k",
    "With ₹2 crore",
    "In a slow market",
    "By year-end,",
    "If you double the budget,",
    "Under the conservative scenario",
)
_COUNTS = (
    "14",
    "900",
    "40",
    "17–35",
    "17-35",
    "17‑35",
    "17‐35",
    "17‒35",
    "17−35",
    "17 — 35",
    "2,400–7,500",
    "1,234",
    "1.234",
    "9 thousand",
    "1-800-555-0199",
    "**900**",
    "*14*",
    "fourteen",
    "a hundred",
)
_UNITS = ("hires", "applications", "new hires", "qualified applications")
_MONEY = ("$66.67", "€66,67", "US$4,000", "USD 66.67", "$8–$25", "$8−$25", "₹2 crore")
_PROSE = (
    "{q} the plan delivers {n} {u}.",
    "This plan projects {n} {u}.",
    "Your plan: {n} {u} {q}.",
    "Expect {n} {u} from this plan.",
    "{q} expect {n} {u}.",
    "Projected {u}: {n}",
    "- Total {u} ({q}): {n}",
)
_MONEY_PROSE = (
    "{q} the plan's CPA is {m}.",
    "This plan's cost per hire is {m}.",
    "Your plan: a cost per applicant of {m}.",
)
_TABLES = (
    "| Channel | Hires |\n|---|---|\n| Acme | 5 |\n| Total | {n} |",
    "| Channel | {u} |\n|---|---|\n| Acme | 5 |\n| **Total** | **{n}** |",
    "### {q}\n\n| Channel | Hires |\n|---|---|\n| Acme | 5 |\n| Grand total | {n} |",
    "| Metric | Value |\n|---|---|\n| Total {u} | {n} |",
    "| Channel | Hires |\n|---|---|\n| Acme | 5 |\n| Total ({q}) | {n} |",
    "| Channel | Hires |\n|---|---|\n| Acme | 5 |\n| Total excl. {q} | {n} |",
    "| First 2 months | Hires |\n|---|---|\n| Nov | 5 |\n| Total | {n} |",
    "  | Channel | Hires |\n  |---|---|\n  | Acme | 5 |\n  | Subtotal | {n} |",
    "| Channel | Hires |\n|---|---|\n| Acme | 5 |\n| Total per role | {n}",
)


def _generated() -> List[str]:
    out = set()
    for tpl, q, n, u in itertools.product(
        _PROSE + _TABLES, _QUALIFIERS, _COUNTS, _UNITS
    ):
        out.add(tpl.format(q=q or "Acme", n=n, u=u).strip())
    for tpl, q, m in itertools.product(_MONEY_PROSE, _QUALIFIERS, _MONEY):
        out.add(tpl.format(q=q or "Here", m=m).strip())
    return sorted(out)


def test_generated_answers_are_only_ever_marked_and_the_guard_is_idempotent():
    texts = _generated()
    assert len(texts) > 1500
    tools = BRIEFS["g60"]
    for text in texts:
        out, st = pf.enforce(text, tools, M60, plan_context=True, user_turns=[M60])
        assert st["rewritten"] == 0, f"REWRITE: {text!r} -> {out!r}"
        _assert_not_mangled("generated", text, out, st)
        again, st2 = pf.enforce(out, tools, M60, plan_context=True, user_turns=[M60])
        assert again == out and st2["rewritten"] == 0, f"not idempotent: {text!r}"


def test_an_exact_total_row_is_marked_and_keeps_its_layout():
    """Until 2026-10-18 an exact channel-table Total row was REWRITTEN to the
    engine's range; it fired on totals of other measures (round 2, W 9). It is
    now marked like every other row, byte for byte otherwise."""
    tools = BRIEFS["g60"]
    for label in (
        "Total",
        "**Total**",
        "Plan total",
        "GRAND TOTAL",
        "total:",
        "Totals",
    ):
        text = (
            "  | Channel | Applications | Hires |\n  |---|---|---|\n"
            f"  | Acme | 503 | 4 |\n  | {label}  |  900 |  14  |"
        )
        out, st = pf.enforce(text, tools, M60, plan_context=True)
        lines = _split(out)[0].split("\n")
        assert st["rewritten"] == 0 and "corrected" not in out.lower(), label
        assert lines[:2] == text.split("\n")[:2]
        assert lines[2] == "  | Acme | 503† | 4† |"
        assert lines[3] == f"  | {label}  |  900† |  14†  |"
        again, _ = pf.enforce(out, tools, M60, plan_context=True)
        assert again == out
    # a total row is held to the PLAN's figures: 14 (one channel's own) and 12
    # (another's) do not excuse it, the engine's range and the user's goal do
    for value, msg, marked in (
        ("14", M60, True),
        ("12", M60, True),
        ("17–35", M60, False),
        ("40", M60 + "; we need 40 hires", False),
    ):
        text = f"| Channel | Hires |\n|---|---|\n| Overall | {value} |"
        out, st = pf.enforce(text, tools, msg, plan_context=True)
        assert (st["marked"] > 0) == marked, (value, out)


def test_unicode_dashes_are_range_separators_everywhere():
    tools = BRIEFS["g60"]
    for dash in "‐‑‒–—−-":
        for text in (
            f"This plan projects 17{dash}35 hires.",
            f"Projected hires: 17{dash}35",
            f"This plan's CPA is $8{dash}$25.",
            f"| Channel | Hires |\n|---|---|\n| Total | 17{dash}35 |",
        ):
            out, st = pf.enforce(text, tools, M60, plan_context=True)
            assert _split(out)[0] == text and st["marked"] == st["rewritten"] == 0, text
        out, st = pf.enforce(
            f"This plan delivers 9{dash}14 hires.", tools, M60, plan_context=True
        )
        assert _split(out)[0] == f"This plan delivers 9{dash}14† hires."


def test_a_phone_number_or_an_id_is_left_byte_identical():
    tools = BRIEFS["g60"]
    for text in (
        "Questions about new hires: 1-800-555-0199.",
        "Applications: call 1‑800‑555‑0199 today.",
        "| Metric | Value |\n|---|---|\n| Hires hotline | 1-800-555-0199 |",
    ):
        out, st = pf.enforce(text, tools, M60, plan_context=True)
        assert out == text and st["marked"] == 0, out


# ---------------------------------------------------------------------------
# Linear time, bounded work
# ---------------------------------------------------------------------------
def test_one_sentence_with_thousands_of_claims_costs_linear_work(monkeypatch):
    """Round 3 measured 438 s for 6,000 claims in one sentence (each claim
    re-scanned its whole clause). The clause context is now read once per
    clause, and the work per answer is capped."""
    calls = {"n": 0}
    real = pf._SKIP_RE

    class _Counting:
        def search(self, s, *a):
            calls["n"] += 1
            return real.search(s, *a)

    monkeypatch.setattr(pf, "_SKIP_RE", _Counting())
    sentence = (
        "This plan delivers " + " and ".join(f"{i} hires" for i in range(1, 6000)) + "."
    )
    t0 = time.perf_counter()
    out, st = pf.enforce(sentence, BRIEFS["g60"], "plan $60k", plan_context=True)
    elapsed = time.perf_counter() - t0
    assert calls["n"] == 1  # one clause, one read of its context
    assert st["claims"] <= pf.MAX_CHECKS_PER_ANSWER
    # about 0.2 s on the dev machine (the brief's budget: under ~1 s)
    assert elapsed < 4.0, elapsed


def test_a_very_long_answer_and_table_stay_fast():
    para = (
        "Programmatic yields 2,961 applications. Global boards bring 14 hires. "
        "This plan delivers 900 applications and 14 hires. "
        "The median salary is $41,000; CPC runs $0.56. "
    )
    table = "| Channel | Applications | Hires |\n|---|---|---|\n" + "".join(
        f"| Ch{i} | {i * 7} | {i} |\n" for i in range(5000)
    )
    for text in (para * 2000, table):
        t0 = time.perf_counter()
        pf.enforce(text, BRIEFS["g60"], "plan $60k", plan_context=True)
        # about 0.4 s / 0.3 s on the dev machine: the bound is 20x+
        assert time.perf_counter() - t0 < 8.0


def test_a_300_store_table_keeps_its_total_row_marked():
    """Round 2: the 600-check cap tripped inside an ordinary 300-store table
    (about 1.7k tokens) and its fabricated Total row went unmarked. The cap is
    now 5,000 and total rows are checked first."""
    rows = "".join(f"| Store {i} | {i % 50 + 60} | {i % 7 + 2} |\n" for i in range(300))
    for n_rows in (300, 3000):  # the second one trips the cap
        rows = "".join(
            f"| Store {i} | {i % 50 + 60} | {i % 7 + 2} |\n" for i in range(n_rows)
        )
        text = (
            "| Store | Applications | Hires |\n|---|---|---|\n"
            + rows
            + "| Total | 900 | 14 |"
        )
        out, _ = pf.enforce(text, BRIEFS["g60"], M60, plan_context=True)
        assert "| Total | 900† | 14† |" in out, n_rows
        assert out.count(pf.CAP_NOTE) == (1 if n_rows == 3000 else 0)


def test_the_cap_note_appears_exactly_once_and_code_does_not_use_the_budget():
    tools = BRIEFS["g60"]
    many = "\n".join(f"- Store {i}: about {i % 7 + 2} hires" for i in range(6000))
    out, st = pf.enforce(many, tools, M60, plan_context=True)
    assert out.count(pf.CAP_NOTE) == 1 and st["marked"] == pf.MAX_CHECKS_PER_ANSWER
    again, _ = pf.enforce(out, tools, M60, plan_context=True)
    assert again == out and again.count(pf.CAP_NOTE) == 1
    # under the cap: no note
    out, _ = pf.enforce(many[:2000], tools, M60, plan_context=True)
    assert pf.CAP_NOTE not in out
    # 6,000 figures in a code fence are never checked and never counted
    fence = "```\n" + "\n".join(f"{i} applications" for i in range(6000)) + "\n```"
    tail = "\n\nOverall, this plan delivers 900 applications and 14 hires."
    out, _ = pf.enforce(fence + tail, tools, M60, plan_context=True)
    assert _split(out)[0].endswith("delivers 900† applications and 14† hires.")
    assert pf.CAP_NOTE not in out


# ---------------------------------------------------------------------------
# The legend
# ---------------------------------------------------------------------------
def test_the_legend_and_the_basis_line_are_plain_and_not_us_only():
    import us_only_markers

    assert (
        nova._T3_UNVERIFIED_FOOTER == "† estimate not traced to Nova's planning engine"
    )
    fig = pf.collect(BRIEFS["g60"])[0][0]
    for line in (
        nova._T3_UNVERIFIED_FOOTER,
        pf.BEST_CASE_SENTENCE,
        pf._basis_line(fig),
    ):
        assert "plan-efficiency" not in line and "floor" not in line, line
        assert not us_only_markers.has_us_only_marker(line), line


def test_a_dagger_always_comes_with_exactly_one_legend(engine):
    case = ("lg", "legend", "g60", (), M60, "", "U", "")
    for text in (
        "This plan delivers 900 applications.",  # the guard's own mark
        "This plan delivers 900 applications†.",  # the model's own dagger
        "This plan delivers 900 applications.\n\n" + nova._T3_UNVERIFIED_FOOTER,
    ):
        out = _pipeline(engine, case, text)
        assert out.count(nova._T3_UNVERIFIED_FOOTER) == 1, out


# ---------------------------------------------------------------------------
# No tool in the turn, a budget earlier in the conversation
# ---------------------------------------------------------------------------
def test_a_no_tool_follow_up_in_a_budget_conversation_is_marked():
    out, st = pf.enforce(
        "This plan delivers 900 applications and 14 hires.",
        [],
        "how many hires will this get?",
        user_turns=[M60, "how many hires will this get?"],
    )
    assert _split(out)[0] == "This plan delivers 900† applications and 14† hires."
    # with no budget anywhere and no plan in the question, nothing is claimed
    out, st = pf.enforce(
        "This plan delivers 900 applications.", [], "how many hires will this get?"
    )
    assert out == "This plan delivers 900 applications." and st["marked"] == 0


# ---------------------------------------------------------------------------
# Cache keys keep the decimal point and the currency
# ---------------------------------------------------------------------------
_PAIRS = (("$1.5k", "$15k"), ("$1.5M", "$15M"), ("$0.5k", "$5k"), ("€15k", "$15k"))


@pytest.mark.parametrize("a,b", _PAIRS)
def test_two_briefs_differing_only_in_the_amount_get_different_cache_keys(a, b):
    brief = "make a {} media plan for nurses in Houston"
    qa, qb = brief.format(a), brief.format(b)
    assert nova._normalize_cache_key(qa) != nova._normalize_cache_key(qb)
    assert nova_cache._normalize_query(qa) != nova_cache._normalize_query(qb)
    # the same amount written the same way still shares a key
    assert nova._normalize_cache_key(qa) == nova._normalize_cache_key(qa.upper())


@pytest.fixture
def live_caches(monkeypatch, tmp_path):
    """Both response caches live, in memory only."""
    import llm_router

    monkeypatch.setattr(nova, "_response_cache", {})
    monkeypatch.setattr(nova, "RESPONSE_CACHE_FILE", tmp_path / "resp.json")
    monkeypatch.setattr(nova, "_upstash_enabled", False)
    monkeypatch.setattr(nova_cache, "_memory_cache", {})
    monkeypatch.setattr(nova_cache, "_supabase_enabled", False)
    monkeypatch.setattr(nova_cache, "_upstash_enabled", False)
    return llm_router


_SPELLED = {
    "$1.5k": ("$1.5k", "$1,500"),
    "$15k": ("$15k", "$15,000"),
    "$1.5M": ("$1.5M", "$1,500,000"),
    "$15M": ("$15M", "$15,000,000"),
    "$0.5k": ("$0.5k", "$500"),
    "$5k": ("$5k", "$5,000"),
}


def _names(text: str, amount: str) -> bool:
    return any(
        re.search(rf"(?<![\d.,]){re.escape(form)}(?![\d,]|\.\d)", text)
        for form in _SPELLED[amount]
    )


@pytest.mark.parametrize("a,b", _PAIRS[:3])
def test_chat_never_serves_one_amounts_answer_to_another(
    engine, live_caches, monkeypatch, a, b
):
    """Round 3 proved a $1.5k brief was served the cached $15k answer through
    chat(): both keys normalised to "15k". A fake provider answers each brief
    about its own amount (the tool-loop answer is cached); the second brief must
    get an answer about ITS amount, not the first brief's cached one."""
    import uuid

    budgets = {"$1.5k": 1500, "$15k": 15000, "$1.5M": 1.5e6, "$15M": 1.5e7,
               "$0.5k": 500, "$5k": 5000}  # fmt: skip

    def fake_call_llm(*args, **kw):
        """A provider that calls the projection (and the recommender) for the
        brief's budget, then writes a plan for that budget from the result."""
        msgs = kw.get("messages") or (args[0] if args else [])
        if not kw.get("tools"):
            return {"provider": "stub", "model": "stub", "text": "{}"}
        users = [str(m.get("content")) for m in msgs if m.get("role") == "user"]
        amount = next((x for u in users for x in (a, b) if x in u), "?")
        tool_msgs = [m for m in msgs if m.get("role") == "tool"]
        if not tool_msgs:
            brief = {"budget": budgets.get(amount, 0), "industry": "healthcare",
                     "roles": ["Registered Nurse"], "role": "Registered Nurse",
                     "locations": ["Houston, TX"]}  # fmt: skip
            return {
                "provider": "stub",
                "model": "stub",
                "text": "",
                "raw_message": {"content": None},
                "tool_calls": [
                    {"id": f"t{i}", "type": "function",
                     "function": {"name": name, "arguments": json.dumps(brief)}}
                    for i, name in enumerate(("query_budget_projection", "recommend_channels"))
                ],  # fmt: skip
            }
        proj = {}
        for m in tool_msgs:
            try:
                proj = json.loads(m.get("content") or "{}").get("projection") or proj
            except (ValueError, AttributeError):
                continue
        t = proj.get("text") or {}
        return {
            "provider": "stub",
            "model": "stub",
            "text": (
                f"### Media Plan: Registered Nurses -- Houston\n**Budget: {amount}**\n\n"
                f"Here is how I would split a {amount} budget. This plan projects "
                f"{t.get('applications')} applications and {t.get('hires')} hires at "
                f"{t.get('cpa')} per application.\n\n"
                "| Channel | Budget | % | Why |\n|---|---|---|---|\n"
                "| Programmatic | 36% | 36% | -- |\n| Niche boards | 30% | 30% | -- |\n"
            ),
        }

    monkeypatch.setattr(live_caches, "call_llm", fake_call_llm)
    # one warm-up turn: a cold first turn (lazy imports in the tool loop) can
    # outlast the loop's budget and fall to the uncached rule-based route
    engine.chat(
        f"make a $40k media plan for nurses in Houston {uuid.uuid4().hex}", [], None
    )
    tag = uuid.uuid4().hex[:6]
    # the whole-number brief first: its answer is the one the cache keeps
    first = engine.chat(f"make a {b} media plan for nurses in Houston {tag}", [], None)
    second = engine.chat(f"make a {a} media plan for nurses in Houston {tag}", [], None)
    assert _names(first["response"], b), first["response"][:300]
    assert _names(second["response"], a), second["response"][:300]
    assert not _names(
        second["response"], b
    ), "the second brief got the first one's answer"
    assert "Here is how I would split" in first["response"]  # the cached route


# ---------------------------------------------------------------------------
# The two text paths that used to bypass the guard
# ---------------------------------------------------------------------------
def test_a_learned_answer_goes_through_the_guard(engine, monkeypatch):
    monkeypatch.setattr(
        nova,
        "_check_learned_answers",
        lambda q: {
            "response": "A plan like this delivers 900 applications and 14 hires.",
            "confidence": 0.9,
            "sources": ["Joveo Knowledge Base (learned answers)"],
            "tools_used": [],
            "cached": True,
        },
    )
    out = engine.chat(M60 + " learned-path", [], None)["response"]
    assert "900† applications" in out and "14† hires" in out
    assert out.count(nova._T3_UNVERIFIED_FOOTER) == 1


def test_the_quality_backfill_is_the_knowledge_base_and_is_not_daggered(
    engine, monkeypatch
):
    """The quality backfill appends knowledge-base passages the system retrieved
    (vector_search over the KB), not text the model wrote: its ranges are the
    KB's own, so they are not marked "not traced". The model's fabricated plan
    total in the same answer is marked (the reply was guarded before)."""
    import vector_search

    class _IQ:
        def chat(self, user_message, conversation_history=None, **kw):
            nova._set_turn_tool_capture(
                nova._new_turn_state(user_message, [], session_id="")
            )
            try:
                return engine._apply_t3_postprocessing(
                    {
                        "response": "This plan delivers 900 applications and 14 hires.",
                        "sources": [],
                        "confidence": 0.9,
                        "tools_used": ["query_budget_projection"],
                        "_tool_results_raw": list(BRIEFS["g60"]),
                    },
                    user_message,
                )
            finally:
                nova._set_turn_tool_capture(None)

    monkeypatch.setattr(nova, "_get_iq", lambda: _IQ())
    monkeypatch.setattr(
        nova, "_enrich_response", lambda **kw: (kw["response_text"], [], 5)
    )
    kb = (
        "Retail CPA ranges $8-$25 per application; programmatic CPA for nurses runs "
        "$12 to $30 in Texas, about 57 applications per hire."
    )
    monkeypatch.setattr(vector_search, "search_bounded", lambda *a, **k: [{"text": kb}])
    out = nova.handle_chat_request({"message": M60})["response"]
    assert "### Additional Data" in out and kb in out  # byte for byte, no dagger
    assert "900† applications" in out and "14† hires" in out
    assert out.count(nova._T3_UNVERIFIED_FOOTER) == 1
