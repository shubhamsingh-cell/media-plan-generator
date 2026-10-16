"""Every statistic the plan copilot shows a user must trace to a repo data file.

The copilot (``plan_copilot.py``, served by ``/api/copilot/nudge`` and Nova's
plan nudges) used to print figures that no file in the repo carried: "23% more
qualified applicants", "Remote hiring could save 20-25%", "Industry benchmark
is $6,500/hire", "LinkedIn (85%)", "Only 15% of passive candidates engage",
"Los Angeles cost index: 1.25x" (the benchmark set says 1.24x). A recruiter who
quotes one of those to a client is quoting nothing.

The rule these tests pin: a figure may appear in user-facing copy only if it is
(a) in ALLOWED_STATS below, which names the repo data file and path it is read
from, and (b) shown next to the source name. Everything else is qualitative.

Two layers, so neither a new literal nor a new code path slips past:
  1. a static scan of every string literal / f-string in plan_copilot.py;
  2. a behavioural sweep that renders real nudges across roles, metros,
     budgets, channels and durations and scans the rendered text.
"""

from __future__ import annotations

import ast
import json
import os
import re
import sys
from typing import Dict, Iterator, List, Tuple

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import plan_copilot as pc  # noqa: E402

SOURCE_PATH = os.path.join(ROOT, "plan_copilot.py")
# The data file's own short names for the sources a metro entry cites. The copy
# must name exactly the sources that entry lists ("blended C2ER and BLS public
# data" for Los Angeles), never a house label.
SHORT_NAMES = {"coli_c2er_2024": "C2ER", "bls_cpi_metro_2025": "BLS", "appcast_2025": "Appcast"}


def expected_label(source_ids):
    names = [SHORT_NAMES[i] for i in source_ids]
    joined = names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]
    return f"blended {joined} public data"


LABEL = expected_label(["coli_c2er_2024", "bls_cpi_metro_2025"])  # Los Angeles
PLACEHOLDER = "<N>"

# Anything that reads as a statistic: 23%, 1.24x, 20-25%, 42 days, 45+, $6,500.
STAT_RE = re.compile(
    r"(?:\d[\d.,]*|<N>)\s*(?:-\s*(?:\d[\d.,]*|<N>)\s*)?"
    r"(?:%|×|x\b|\s*percent\b|\s*days?\b)"
    r"|\$\s*(?:\d|<N>)"
    r"|\b\d+\+"
)

# Each allowed figure: the exact copy it may appear in (as the static scan sees
# it, "<N>" standing for an interpolated value) -> the repo data it is read from.
ALLOWED_STATS: Dict[str, Dict[str, object]] = {
    "cost per application runs about <N>x the national median (<N>)": {
        "file": "data/recruitment_benchmarks_2026_deep.json",
        "path": ("by_metro", "*", "metro_cpa_mult"),
        "why": "the metro's own multiplier, printed with 2 decimals",
    },
    "so your budget stretches like ~$<N> in a median market": {
        "file": "data/recruitment_benchmarks_2026_deep.json",
        "path": ("by_metro", "*", "metro_cpa_mult"),
        "why": "the user's own budget divided by that same multiplier",
    },
}


def _read_bytes_json(rel: str) -> dict:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as fh:
        return json.load(fh)


def _docstring_nodes(tree: ast.AST) -> set:
    found = set()
    for node in ast.walk(tree):
        if isinstance(
            node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
        ):
            body = getattr(node, "body", [])
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                found.add(id(body[0].value))
    return found


def _non_user_facing_nodes(tree: ast.AST) -> set:
    """Strings inside logger.* / re.* calls are never shown to a user."""
    skipped = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        owner = func.value.id if isinstance(func, ast.Attribute) and isinstance(
            func.value, ast.Name
        ) else ""
        if owner in ("logger", "logging", "re"):
            for sub in ast.walk(node):
                skipped.add(id(sub))
    return skipped


def _render(node: ast.AST) -> str:
    """A literal or f-string as the reader sees it; interpolations -> <N>
    (the metro's multiplier AND its source label, both read from the data)."""
    if isinstance(node, ast.Constant):
        return str(node.value)
    parts: List[str] = []
    for value in node.values:  # type: ignore[attr-defined]
        if isinstance(value, ast.Constant):
            parts.append(str(value.value))
        else:
            parts.append(PLACEHOLDER)
    return "".join(parts)


def _module_strings() -> Iterator[Tuple[int, str]]:
    with open(SOURCE_PATH, encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    skip = _docstring_nodes(tree) | _non_user_facing_nodes(tree)
    inside_fstring = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            for child in ast.walk(node):
                if child is not node:
                    inside_fstring.add(id(child))
    for node in ast.walk(tree):
        if id(node) in skip or id(node) in inside_fstring:
            continue
        if isinstance(node, (ast.Constant, ast.JoinedStr)):
            if isinstance(node, ast.Constant) and not isinstance(node.value, str):
                continue
            yield node.lineno, _render(node)


def _strip_allowed(text: str) -> str:
    for fragment in ALLOWED_STATS:
        text = text.replace(fragment, "")
    return text


# --------------------------------------------------------------------------
# Layer 1: static scan of plan_copilot.py
# --------------------------------------------------------------------------


def test_scan_actually_sees_the_copilot_strings():
    """Guard the guard: a broken walker would pass everything."""
    seen = [text for _, text in _module_strings()]
    assert len(seen) > 40
    assert any("good channels to start with" in t for t in seen)
    assert any("national median" in t for t in seen)


def test_no_unsourced_statistic_in_any_string_literal():
    offenders = []
    for lineno, text in _module_strings():
        for match in STAT_RE.finditer(_strip_allowed(text)):
            offenders.append(f"plan_copilot.py:{lineno}: {match.group(0)!r} in {text!r}")
    assert not offenders, (
        "statistic in copilot copy with no ALLOWED_STATS source:\n  "
        + "\n  ".join(offenders)
    )


def test_scan_catches_the_strings_that_used_to_ship():
    """The old copy must trip the scanner (so the test above can fail)."""
    old_copy = [
        "Multi-title campaigns typically see 23% more qualified applicants.",
        "Median CPA is ~1.25x the baseline.",
        "Remote hiring could save 20-25%",
        "Consider increasing by 20-30% for better reach",
        "Industry benchmark is $6,500/hire",
        "Only 15% of passive candidates engage in the first 30 days.",
        "Technical roles average 42 days to fill",
        "Executive search requires relationship building; 90+ day avg time-to-fill",
        "expanding your talent pool by 3-5x",
        "LinkedIn (85%)",
    ]
    for text in old_copy:
        assert STAT_RE.search(_strip_allowed(text)), text


@pytest.mark.parametrize("fragment", sorted(ALLOWED_STATS))
def test_each_allowed_statistic_is_still_in_the_module(fragment):
    """No stale allow-list entry: an entry nobody prints would hide a future
    unsourced claim worded the same way."""
    assert any(fragment in text for _, text in _module_strings()), fragment


@pytest.mark.parametrize("fragment", sorted(ALLOWED_STATS))
def test_each_allowed_statistic_resolves_to_a_data_file(fragment):
    spec = ALLOWED_STATS[fragment]
    data = _read_bytes_json(str(spec["file"]))
    top, wildcard, leaf = spec["path"]  # type: ignore[misc]
    assert wildcard == "*"
    entries = [
        entry
        for key, entry in data[top].items()
        if not key.startswith("_") and isinstance(entry, dict)
    ]
    values = [e[leaf] for e in entries if isinstance(e.get(leaf), (int, float))]
    assert len(values) >= 40, f"{spec['file']} {top}.*.{leaf} has {len(values)} values"


def test_a_stat_bearing_fragment_names_its_source_in_the_same_sentence():
    for fragment in ALLOWED_STATS:
        if "x the national median" in fragment:
            assert fragment.endswith("(<N>)")  # a parenthesised source follows the figure


# --------------------------------------------------------------------------
# Layer 2: render real nudges and scan what a user would read
# --------------------------------------------------------------------------

ROLES = [
    "Software Engineer",
    "Data Scientist",
    "Product Manager",
    "Sales Manager",
    "Marketing Manager",
    "VP of Operations",
    "Registered Nurse",
    "Warehouse Manager",
    "Barista",
]
BUDGETS = ["$2,000", "$9,000", "$50,000", "$250,000", "$2,000,000"]
DURATIONS = ["1-3 months", "3-6 months", "6-12 months", "Under 1 month"]
CHANNELS = ["Indeed", "LinkedIn", "Glassdoor", "Indeed, LinkedIn", "Programmatic"]
ALLOWED_RENDERED = [
    re.compile(
        r"cost per application runs about (\d\.\d\d)x the national median "
        r"\(blended [A-Za-z0-9, ]+ public data\)"
    ),
    re.compile(r"so your budget stretches like ~\$([\d,]+) in a median market"),
]


METRO_SOURCES: Dict[str, List[str]] = {}


def _metros() -> List[Tuple[str, float]]:
    out = []
    metros = _read_bytes_json("data/recruitment_benchmarks_2026_deep.json")["by_metro"]
    for key, entry in metros.items():
        if key.startswith("_") or not isinstance(entry, dict):
            continue
        city = str(entry["msa"]).split("-")[0].strip().replace(".", "")
        state = key.rsplit("_", 1)[-1].upper()
        out.append((f"{city}, {state}", float(entry["metro_cpa_mult"])))
        METRO_SOURCES[f"{city}, {state}"] = list(entry["source_ids"])
    return out


def _all_messages() -> List[Tuple[str, str]]:
    """(label, message) for a broad matrix of wizard states."""
    found: List[Tuple[str, str]] = []

    def collect(field, value, ctx):
        for nudge in pc.get_copilot_nudges_multi(field, value, ctx):
            found.append((f"{field}={value!r} ctx={ctx}", nudge["message"]))
            found.append((f"{field}={value!r} source", nudge["source"]))

    metros = _metros()
    for role in ROLES:
        collect("job_title", role, {})
        collect("job_title", f"{role}, Product Designer", {})
        for duration in DURATIONS:
            collect("duration", duration, {"roles": [role]})
        for channel in CHANNELS:
            collect("channel", channel, {"roles": [role]})
        for budget in BUDGETS:
            collect("budget", budget, {"roles": [role]})
    for city, _ in metros:
        collect("location", city, {"roles": ["Software Engineer"]})
        collect("location", city, {"roles": ["Registered Nurse"]})
        collect("budget", "$50,000", {"roles": ["Software Engineer"], "locations": [city]})
    for place in ("Remote", "London", "Berlin", "Portland, ME", "Anywhere"):
        collect("location", place, {"roles": ["Software Engineer"]})
        collect(
            "budget", "$50,000", {"roles": ["Software Engineer"], "locations": [place]}
        )
    return found


def _strip_rendered_allowed(text: str) -> str:
    for pattern in ALLOWED_RENDERED:
        text = pattern.sub("", text)
    return text


def test_the_sweep_produces_a_real_sample():
    messages = _all_messages()
    assert len(messages) > 400
    assert any("national median" in m for _, m in messages)
    assert any("good channels to start with" in m for _, m in messages)


def test_no_rendered_nudge_contains_an_unsourced_statistic():
    offenders = []
    for label, message in _all_messages():
        for match in STAT_RE.finditer(_strip_rendered_allowed(message)):
            offenders.append(f"{label}: {match.group(0)!r} in {message!r}")
    assert not offenders, "\n  ".join(offenders[:10])


def test_every_rendered_multiplier_equals_the_data_files_value():
    """The 1.24x shown for Los Angeles is metro_cpa_mult, not a typed copy."""
    expected = dict(_metros())
    checked = 0
    for city, mult in expected.items():
        messages = [
            n["message"]
            for n in pc.get_copilot_nudges_multi(
                "location", city, {"roles": ["Software Engineer"]}
            )
            if "national median" in n["message"]
        ]
        assert len(messages) == 1, (city, messages)
        shown = ALLOWED_RENDERED[0].search(messages[0])
        assert shown, messages[0]
        assert f"{float(shown.group(1)):.2f}" == f"{mult:.2f}", (city, messages[0], mult)
        assert f"({expected_label(METRO_SOURCES[city])})" in messages[0], (city, messages[0])
        checked += 1
    assert checked >= 40


def test_los_angeles_says_what_the_benchmark_set_says():
    """The old table said 1.25x; the data file says 1.24x."""
    text = " ".join(
        n["message"]
        for n in pc.get_copilot_nudges_multi("location", "Los Angeles", {})
    )
    assert f"1.24x the national median ({LABEL})" in text
    assert "1.25" not in text


def test_budget_stretch_figure_is_the_users_budget_over_the_data_multiplier():
    nudges = pc.get_copilot_nudges_multi(
        "budget",
        "$50,000",
        {"roles": ["Registered Nurse"], "locations": ["Los Angeles, CA"]},
    )
    text = " ".join(n["message"] for n in nudges)
    assert f"1.24x the national median ({LABEL})" in text
    assert f"~${round(50000 / 1.24):,} in a median market" in text


@pytest.mark.parametrize(
    "place", ["London", "Berlin", "Remote", "Portland, ME", "Anywhere"]
)
def test_places_without_a_benchmark_get_no_cost_claim(place):
    """No data for London or Portland, ME (the table's Portland is OR): say
    nothing about cost rather than guess."""
    for field, ctx in (
        ("location", {"roles": ["Registered Nurse"]}),
        ("budget", {"roles": ["Registered Nurse"], "locations": [place]}),
    ):
        value = place if field == "location" else "$50,000"
        for nudge in pc.get_copilot_nudges_multi(field, value, ctx):
            assert "national median" not in nudge["message"], nudge["message"]


def test_remote_option_is_advice_not_a_savings_figure():
    texts = [
        n["message"]
        for n in pc.get_copilot_nudges_multi(
            "location", "Los Angeles, CA", {"roles": ["Software Engineer"]}
        )
        if "remote" in n["message"].lower()
    ]
    assert texts, "the remote-option advice is gone"
    for text in texts:
        assert "save" not in text.lower()
        assert not re.search(r"\d\s*%", text)


def test_channel_suggestions_name_channels_without_scores():
    texts = [
        n["message"]
        for role in ROLES
        for n in pc.get_copilot_nudges_multi("channel", "Indeed", {"roles": [role]})
    ]
    assert texts
    for text in texts:
        assert "%" not in text
        assert "ffectiveness" not in text  # "Effectiveness score ..." is gone
        assert "quality-of-hire" not in text


def test_budget_never_claims_a_fit_it_cannot_know():
    """The wizard sends no hiring volume to the copilot, so 'well matched to
    your hiring volume' was true of every budget, $2M included."""
    for role in ROLES:
        for budget in BUDGETS:
            for nudge in pc.get_copilot_nudges_multi("budget", budget, {"roles": [role]}):
                text = nudge["message"].lower()
                assert "well matched" not in text
                assert "well positioned" not in text
                assert "benchmark is" not in text


def test_a_missing_benchmark_file_degrades_to_no_cost_claim(monkeypatch, tmp_path):
    monkeypatch.setattr(pc, "BENCHMARK_FILE", str(tmp_path / "absent.json"))
    monkeypatch.setattr(pc, "_geo_table", None)
    try:
        assert pc.get_copilot_nudges_multi("location", "Los Angeles", {}) == []
    finally:
        monkeypatch.setattr(pc, "_geo_table", None)


def test_the_metro_source_label_says_what_the_data_file_says():
    """The multipliers are C2ER's cost-of-living index and BLS metro CPI (plus
    Appcast for some metros), blended, per by_metro.<metro>.source_ids -- not a
    Joveo survey. The copy names the sources the entry lists, as the file does."""
    data = _read_bytes_json("data/recruitment_benchmarks_2026_deep.json")
    entries = {k: v for k, v in data["by_metro"].items() if not k.startswith("_")}
    ids = {sid for entry in entries.values() for sid in entry["source_ids"]}
    assert ids <= set(SHORT_NAMES), ids - set(SHORT_NAMES)  # a new source must be named here
    names = {s["id"]: s["name"] + " " + s["publisher"] for s in data["metadata"]["sources"]}
    assert "C2ER" in names["coli_c2er_2024"] or "Council for Community" in names["coli_c2er_2024"]
    assert "BLS" in names["bls_cpi_metro_2025"] or "Bureau of Labor" in names["bls_cpi_metro_2025"]
    text = " ".join(
        n["message"] for n in pc.get_copilot_nudges_multi("location", "Los Angeles, CA", {})
    )
    assert "(blended C2ER and BLS public data)" in text
    source = open(SOURCE_PATH, encoding="utf-8").read()
    assert "Joveo 2026 benchmark set" not in source and "BENCHMARK_SOURCE_LABEL" not in source


def test_a_nurse_is_not_told_to_consider_a_physician_or_pharmacist():
    for title in ("Registered Nurse", "nurse", "ICU Nurse", "Nurse Practitioner"):
        nudges = pc.get_copilot_nudges_multi("job_title", title, {})
        text = " ".join(n["message"] for n in nudges)
        assert "Related titles to consider" in text, title
        for wrong in ("Physician", "Pharmacist", "Clinical Researcher", "Healthcare Administrator"):
            assert wrong not in text, (title, text)
    # other families keep their own list
    text = " ".join(
        n["message"] for n in pc.get_copilot_nudges_multi("job_title", "Software Engineer", {})
    )
    assert "Frontend Developer" in text


def test_no_budget_nudge_carries_a_figure_the_wizard_could_apply_as_the_budget():
    """Round-5: the click-to-apply path read the first "$" figure out of the
    text (the user's own budget / a metro multiplier) and "applied" it. The
    wizard no longer applies budgets at all (see test_wizard_copy_claims), and
    the text that figure sits in stays an informational equivalent."""
    nudges = pc.get_copilot_nudges_multi(
        "budget", "$35,000", {"roles": ["Registered Nurse"], "locations": ["Los Angeles, CA"]}
    )
    stretch = [n for n in nudges if "stretches like" in n["message"]]
    assert stretch and stretch[0]["type"] == "suggestion"
    assert "(blended C2ER and BLS public data)" in stretch[0]["message"]
