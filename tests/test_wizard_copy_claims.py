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
