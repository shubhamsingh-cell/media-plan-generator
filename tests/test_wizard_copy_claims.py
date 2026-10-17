"""No unsourced figure in the wizard's own copy (the browser-side twin of
tests/test_plan_copilot_claims.py).

The trust lens kept finding the same class of defect one surface at a time: a
hand-typed percentage, multiplier or dollar benchmark in user-facing copy that
no data file in the repo carries ("expect ~15% higher CPCs", "increasing budget
by 15-20%", "Supports 40+ countries", "High ~$7K/hire" graded against a typed
table, "Avg CPA $9"). plan_copilot.py is already guarded; this guards the rest
of what the wizard shows:

  * every string literal in templates/partials/index/body_*_js.html (and
    body_footer_scripts.html), comments and regex literals skipped;
  * the visible text and the title / aria-label / placeholder / alt attributes
    of templates/partials/index/body_content.html.

Round 6 widened the net to the two places the first version missed:

  * routes/*.py -- every user-facing string literal AND every numeric-literal
    table (the "Market Pricing Benchmarks" panel's figures lived in float dicts
    typed into routes/pricing.py, not in strings, so a string scan alone passed);
  * data/channels_db.json -- the JSON-backed text the wizard renders on step 2
    (job categories, industries): every field the JS reads must be listed in
    RENDERED_JSON_FIELDS and must carry no figure.

A literal that contains a figure (``15%``, ``1.4x``, ``$7K``, ``40+``) must be
matched by an allow-list entry below that says WHY it is not a claim:

  layout   CSS / SVG geometry, not copy ("width:100%")
  option   the label of a choice the USER picks (a budget range, a hire volume)
  name     a product / channel name that happens to contain a number
  data     a figure quoted from a data file; the test checks that file still
           carries the quoted text

Figures that are COMPUTED at runtime (``hero(est.est_cph)``, ``${pct}%``) never
appear as a digit in a literal, so they are not flagged: they trace to the
estimate (and the benchmark it returns), which tests/test_cph_benchmark.py pins.
"""

from __future__ import annotations

import ast
import json
import os
import re
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterator, List, Tuple

import pytest

ROOT = Path(__file__).resolve().parent.parent
PARTIALS = ROOT / "templates" / "partials" / "index"

# 15%, 1.4x, 20-25%, $7K / $6,500, 40+ (a count with a plus), 3x
STAT_RE = re.compile(
    r"\d[\d.,]*\s*(?:-\s*\d[\d.,]*\s*)?(?:%|×|x\b)|\$\s*\d|\b\d[\d,]*\+"
)

JS_FILES = sorted(PARTIALS.glob("body_*_js.html")) + [PARTIALS / "body_footer_scripts.html"]
HTML_FILES = [PARTIALS / "body_content.html"]
ATTRS = ("title", "aria-label", "placeholder", "alt", "data-tooltip", "aria-description")


# --------------------------------------------------------------------------
# The allow-list: (kind, regex on the whole literal, why)
# --------------------------------------------------------------------------

ALLOWED = [
    ("layout", r"^\d{1,3}%$", "progress-bar / fill widths"),
    ("layout", r'^<svg width="100%" viewBox', "inline SVG geometry"),
    ("option", r"^(?:< )?\$[\d,]+(?: - \$[\d,]+|\+)?$", "budget range the user can pick"),
    ("option", r"^\$\d+(?:\.\d+)?[KM]$", "quick-amount buttons ($50K ... $4M)"),
    ("option", r"^\d[\d,]*\+ hires$", "hire-volume option (500+ hires)"),
    ("option", r"^Executive \(10\+ yrs\)$", "experience-level option"),
    ("option", r"^\$0$", "the empty-budget counter text"),
    ("name", r"(?i)^Innovative Channels? 2025\+$", "channel category name"),
    (
        "data",
        r"Appcast's 2025 benchmark \(379M clicks, 1,300\+ US employers",
        "data/recruitment_benchmarks_2026_deep.json metadata.sources[appcast_2025].sample_size",
    ),
]

# kind "data": text the cited file must still contain
DATA_NEEDLES = {
    r"Appcast's 2025 benchmark \(379M clicks, 1,300\+ US employers": (
        "data/recruitment_benchmarks_2026_deep.json",
        ("379M clicks", "1,300 employers"),
    ),
}


# --------------------------------------------------------------------------
# Extraction
# --------------------------------------------------------------------------


def js_strings(src: str) -> Iterator[Tuple[int, str]]:
    """(line, text) of every JS string / template literal; comments and regex
    literals skipped. Interpolations inside a template literal become ``<N>``."""
    i, n, line = 0, len(src), 1
    prev = ""
    while i < n:
        c = src[i]
        if c == "\n":
            line += 1
            i += 1
            continue
        if c in " \t\r":
            i += 1
            continue
        if src.startswith("//", i):
            j = src.find("\n", i)
            i = n if j < 0 else j
            continue
        if src.startswith("/*", i):
            j = src.find("*/", i + 2)
            j = n if j < 0 else j + 2
            line += src.count("\n", i, j)
            i = j
            continue
        if c in "'\"":
            j, buf = i + 1, []
            while j < n and src[j] != c and src[j] != "\n":
                if src[j] == "\\" and j + 1 < n:
                    buf.append(src[j + 1])
                    j += 2
                    continue
                buf.append(src[j])
                j += 1
            yield line, "".join(buf)
            i, prev = j + 1, c
            continue
        if c == "`":
            j, buf, start = i + 1, [], line
            while j < n and src[j] != "`":
                if src[j] == "\\" and j + 1 < n:
                    buf.append(src[j + 1])
                    j += 2
                    continue
                if src.startswith("${", j):
                    buf.append("<N>")
                    j, depth = j + 2, 1
                    while j < n and depth:
                        depth += {"{": 1, "}": -1}.get(src[j], 0)
                        j += 1
                    continue
                if src[j] == "\n":
                    line += 1
                buf.append(src[j])
                j += 1
            yield start, "".join(buf)
            i, prev = j + 1, "`"
            continue
        if c == "/" and (prev == "" or prev in "(,=:[!&|?{};"):
            j, in_class = i + 1, False
            while j < n and src[j] != "\n":
                if src[j] == "\\":
                    j += 2
                    continue
                if src[j] == "[":
                    in_class = True
                elif src[j] == "]":
                    in_class = False
                elif src[j] == "/" and not in_class:
                    break
                j += 1
            i = j + 1
            while i < n and src[i].isalpha():
                i += 1
            prev = "/"
            continue
        prev = c
        i += 1


class _Text(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.skip = 0
        self.out: List[Tuple[int, str]] = []

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.skip += 1
        for key, value in attrs:
            if key in ATTRS and value:
                self.out.append((self.getpos()[0], value))

    def handle_endtag(self, tag):
        if tag in ("script", "style") and self.skip:
            self.skip -= 1

    def handle_data(self, data):
        if not self.skip and data.strip():
            self.out.append((self.getpos()[0], re.sub(r"\s+", " ", data.strip())))


def literals() -> List[Tuple[str, int, str]]:
    found: List[Tuple[str, int, str]] = []
    for path in JS_FILES:
        for line, text in js_strings(path.read_text(encoding="utf-8")):
            found.append((path.name, line, text))
    for path in HTML_FILES:
        parser = _Text()
        parser.feed(path.read_text(encoding="utf-8"))
        for line, text in parser.out:
            found.append((path.name, line, text))
    return found


def _allowed(text: str) -> bool:
    return any(re.search(pattern, text) for _, pattern, _ in ALLOWED)


# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------


def test_the_scanner_actually_reads_the_wizard_copy():
    """Guard the guard: a broken walker would pass everything."""
    found = literals()
    assert len(found) > 1500
    files = {name for name, _, _ in found}
    assert {"body_app_js.html", "body_preview_js.html", "body_copilot_js.html", "body_content.html"} <= files
    texts = " ".join(t for _, _, t in found)
    assert "Updating estimate" in texts and "Budget Reality Check" in texts
    assert "Appcast's 2025 benchmark" in texts  # the one quoted figure is seen


def test_no_figure_in_the_wizards_copy_lacks_a_source():
    offenders = [
        f"{name}:{line}: {text[:110]!r}"
        for name, line, text in literals()
        if STAT_RE.search(text) and not _allowed(text)
    ]
    assert not offenders, (
        "a figure in user-facing wizard copy that is neither computed at runtime nor "
        "in ALLOWED (add it only with a data source, or reword it qualitatively):\n  "
        + "\n  ".join(offenders)
    )


def test_the_strings_the_trust_lens_found_would_all_be_caught():
    old_copy = [
        "). Consider increasing budget by 15-20% for competitive visibility.",
        "Q1 start = <b>peak hiring</b> — expect ~15% higher CPCs; front-load budget.",
        "Supports 40+ countries worldwide — include the country name",
        "Avg CPA $9 for Healthcare Medical",
        "Recommended budget: $56,452",
        "Multi-title campaigns typically see 23% more qualified applicants.",
        "Remote hiring could save 20-25%",
        "Industry benchmark is $6,500/hire",
        "up to 12 sources",  # no %/x/$ -> not this scanner's class (covered by its own test)
    ]
    caught = [t for t in old_copy if STAT_RE.search(t) and not _allowed(t)]
    assert caught == old_copy[:-1]


def test_every_allow_list_entry_still_matches_something():
    """A stale entry would silently license a future claim worded the same way."""
    found = [text for _, _, text in literals()]
    for kind, pattern, why in ALLOWED:
        assert any(re.search(pattern, t) for t in found), (kind, pattern, why)


@pytest.mark.parametrize("pattern", sorted(DATA_NEEDLES))
def test_each_quoted_figure_is_still_in_its_data_file(pattern):
    rel, needles = DATA_NEEDLES[pattern]
    with open(os.path.join(ROOT, rel), encoding="utf-8") as fh:
        blob = fh.read()
    for needle in needles:
        assert needle in blob, f"{rel} no longer carries {needle!r}"


def test_the_data_entries_all_have_a_source_check():
    data_patterns = {pattern for kind, pattern, _ in ALLOWED if kind == "data"}
    assert data_patterns == set(DATA_NEEDLES)


def test_the_countries_line_is_computed_not_typed():
    body = (PARTIALS / "body_content.html").read_text(encoding="utf-8")
    assert "{{claim_countries}}" in body
    assert not re.search(r"\b\d+\+? countries", re.sub(r"<!--.*?-->", "", body, flags=re.S))


class _PageHandler:
    """Just enough of the request handler for routes.pages._serve_template."""

    def __init__(self) -> None:
        import io

        self.headers = {}
        self.wfile = io.BytesIO()
        self.sent = {}
        self.status = None

    def send_response(self, code):
        self.status = code

    def send_header(self, key, value):
        self.sent[key] = value

    def end_headers(self):
        pass

    def send_error(self, code, message=None):
        self.status = code


def test_the_served_wizard_page_has_its_proof_tokens_resolved():
    """Composed pages used to skip proof_claims, so the countries token would
    have reached the browser literally."""
    from routes import pages

    handler = _PageHandler()
    pages._serve_template(handler, str(ROOT / "templates"), "index.html")
    body = handler.wfile.getvalue().decode("utf-8")
    assert handler.status == 200
    assert "{{claim_" not in body
    assert re.search(r"job boards cover\s+\d+ countries", body), "countries claim did not render a count"


# ══════════════════════════════════════════════════════════════════════════
# routes/*.py (round 6)
# ══════════════════════════════════════════════════════════════════════════

# + "N days": routes carry time claims too
ROUTE_STAT_RE = re.compile(STAT_RE.pattern + r"|\b\d+\s*(?:-\s*\d+\s*)?days?\b")
ROUTE_FILES = sorted((ROOT / "routes").glob("*.py"))

ROUTE_ALLOWED = [
    ("campaign.py", r"<!DOCTYPE|style=", "layout", "HTML page chrome / inline CSS geometry"),
    (
        "health.py",
        r"per M, 1M ctx|perf traces",
        "internal",
        "admin diagnostics page (vendor price list, Sentry config); never shown in the wizard or a plan",
    ),
]

# A float-valued dict that looks like a price / multiplier table
_TABLE_KEY_HINT = re.compile(r"(cpc|cpa|cph|cost|price|pricing|mult|index|rate|benchmark|budget|salary|wage|fee)", re.I)


def _route_skip_ids(tree: ast.AST) -> set:
    skip = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = getattr(node, "body", [])
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                skip.add(id(body[0].value))
        if isinstance(node, ast.Call):
            func = node.func
            owner = func.value.id if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) else ""
            if owner in ("logger", "logging", "re"):
                skip.update(id(sub) for sub in ast.walk(node))
    return skip


def route_literals() -> List[Tuple[str, int, str]]:
    found = []
    for path in ROUTE_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        skip = _route_skip_ids(tree)
        inside = {id(ch) for n in ast.walk(tree) if isinstance(n, ast.JoinedStr) for ch in ast.walk(n) if ch is not n}
        for node in ast.walk(tree):
            if id(node) in skip or id(node) in inside:
                continue
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                found.append((path.name, node.lineno, node.value))
            elif isinstance(node, ast.JoinedStr):
                text = "".join(str(v.value) if isinstance(v, ast.Constant) else "<N>" for v in node.values)
                found.append((path.name, node.lineno, text))
    return found


def route_float_tables() -> List[str]:
    """Dict literals in routes/*.py that are price / multiplier tables."""
    hits = []
    for path in ROUTE_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            floats = [
                (k.value if isinstance(k, ast.Constant) else "?", v.value)
                for k, v in zip(node.keys, node.values)
                if isinstance(v, ast.Constant) and isinstance(v.value, float)
            ]
            named = [k for k, _ in floats if isinstance(k, str) and _TABLE_KEY_HINT.search(k)]
            if len(floats) >= 3 or named:
                hits.append(f"{path.name}:{node.lineno}: {floats[:4]}")
    return hits


def _route_allowed(name: str, text: str) -> bool:
    return any(f == name and re.search(pattern, text) for f, pattern, _, _ in ROUTE_ALLOWED)


def test_the_route_scanner_reads_the_routes():
    found = route_literals()
    assert len(found) > 300
    assert {"pricing.py", "export.py", "copilot.py"} <= {name for name, _, _ in found}


def test_no_route_string_carries_an_unsourced_figure():
    offenders = [
        f"routes/{name}:{line}: {text[:100]!r}"
        for name, line, text in route_literals()
        if ROUTE_STAT_RE.search(text) and not _route_allowed(name, text)
    ]
    assert not offenders, "\n  ".join(offenders)


def test_no_route_holds_a_hand_typed_price_or_multiplier_table():
    """routes/pricing.py had per-channel cpc_base / cpa_base / cph_base floats, an
    industry multiplier of 2.2 for healthcare and a city index with Los Angeles
    at 1.20 -- none of it in a string, so the string scan could not see it."""
    assert route_float_tables() == []


def test_the_table_detector_would_have_caught_the_old_pricing_table():
    old = ast.parse(
        """
base = [{"name": "Indeed", "cpc_base": 0.50, "cpa_base": 25.0, "cph_base": 3500, "multiplier": 1.0}]
mult = {"healthcare_medical": 2.2, "technology_software": 1.5, "finance_banking": 1.4}
"""
    )
    flagged = 0
    for node in ast.walk(old):
        if isinstance(node, ast.Dict):
            floats = [
                (k.value, v.value)
                for k, v in zip(node.keys, node.values)
                if isinstance(v, ast.Constant) and isinstance(v.value, float) and isinstance(k, ast.Constant)
            ]
            named = [k for k, _ in floats if isinstance(k, str) and _TABLE_KEY_HINT.search(k)]
            flagged += bool(len(floats) >= 3 or named)
    assert flagged == 2


def test_every_route_allow_list_entry_still_matches_something():
    found = route_literals()
    for name, pattern, kind, why in ROUTE_ALLOWED:
        assert any(f == name and re.search(pattern, t) for f, _, t in found), (name, pattern, kind, why)


# ══════════════════════════════════════════════════════════════════════════
# data/channels_db.json -- the JSON-backed text step 2 renders (round 6)
# ══════════════════════════════════════════════════════════════════════════

CHANNELS_DB = ROOT / "data" / "channels_db.json"

# Per section, the fields of each entry the wizard reads and shows. A new field
# in the UI must be listed here, which puts its text under the figure scan below.
RENDERED_JSON_FIELDS = {
    "job_categories": {"icon", "label", "recommended_channels", "joveo_supply_fit", "competitor_channels", "best_practices"},
    "industries": {"label", "typical_roles"},
}
# carry figures the file gives no source for (its healthcare CPA "$8.00 - $35.00"
# sits beside the benchmark file's nursing CPA of $28-$68): never rendered
UNSOURCED_JSON_FIELDS = {"avg_cpa_range", "avg_time_to_fill", "strategy_emphasis"}


def _json_strings(node, path=()):
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _json_strings(value, path + (str(key),))
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from _json_strings(value, path + (str(index),))
    elif isinstance(node, bool):
        return
    elif isinstance(node, (str, int, float)):
        yield ".".join(path), str(node)


def test_the_json_fields_the_wizard_reads_are_all_registered():
    app_js = (PARTIALS / "body_app_js.html").read_text(encoding="utf-8")
    code = re.sub(r"//[^\n]*", "", app_js)
    read = {
        "job_categories": set(),
        "industries": set(),
    }
    # cat.<field> / cat?.<field> inside the job-category functions
    for fn in ("function populateJobCategories(", "function updateJobCategoryInsight("):
        block = code[code.index(fn) :]
        block = block[: block.index("\n  }\n")]
        read["job_categories"].update(re.findall(r"\bcat\??\.([a-z_]+)", block))
    # channelsDB.<section>[key]?.<field>
    for section, field in re.findall(r"channelsDB\??\.(industries|job_categories)\??\.?\[[^\]]+\]\??\.([a-z_]+)", code):
        read[section].add(field)
    # const ind = channelsDB.industries[...]; ind.<field>
    block = code[code.index("const ind = channelsDB.industries[selectedIndustry];") :][:600]
    read["industries"].update(re.findall(r"\bind\??\.([a-z_]+)", block))
    assert read["job_categories"] and read["industries"]
    for section, fields in read.items():
        assert fields <= RENDERED_JSON_FIELDS[section], (section, fields - RENDERED_JSON_FIELDS[section])
    assert not (read["job_categories"] & UNSOURCED_JSON_FIELDS)


def test_no_rendered_channels_db_text_carries_a_figure():
    data = json.loads(CHANNELS_DB.read_text(encoding="utf-8"))
    offenders = []
    for section, fields in RENDERED_JSON_FIELDS.items():
        for key, entry in data[section].items():
            for field in fields:
                for where, text in _json_strings(entry.get(field), (section, key, field)):
                    if ROUTE_STAT_RE.search(text):
                        offenders.append(f"{where}: {text[:90]!r}")
    assert not offenders, "\n  ".join(offenders[:15])


def test_the_unsourced_job_category_figures_exist_but_are_not_rendered():
    """The test above is only meaningful because these fields are really in the
    file: if they are ever removed this note (and UNSOURCED_JSON_FIELDS) can go."""
    data = json.loads(CHANNELS_DB.read_text(encoding="utf-8"))
    present = {f for entry in data["job_categories"].values() for f in entry}
    assert UNSOURCED_JSON_FIELDS <= present
    assert not (UNSOURCED_JSON_FIELDS & set().union(*RENDERED_JSON_FIELDS.values()))


def test_no_wizard_copy_reassures_about_hires_at_the_conservative_end():
    """The hires range divides a media budget by Nova's assumed ALL-IN cost per
    hire; "covers your target even at the conservative end" stated the
    reassurance with the basis nowhere on screen. The basis must travel with
    the claim (hireBasisText / the insight's "if hires cost the assumed ...")."""
    for path in JS_FILES:
        src = path.read_text(encoding="utf-8")
        assert "even at the conservative end" not in src, path.name
    preview = (PARTIALS / "body_preview_js.html").read_text(encoding="utf-8")
    assert "if hires cost the assumed" in preview
    assert "all-in cost per hire (Nova table)" in preview
