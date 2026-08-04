"""Structural CPC-literal citation guard (2026-08-04).

Closes this repo's chronically-recurring "uncited/stale-CPC-fallback"
defect class structurally, instead of per-file: 4 prior "permanent" fixes
(aadf231, 208f500, 2a2d983, 20886c0/ae2047b) each landed on a different
file and each missed at least one other file with the same problem. This
test replaces "did someone remember to grep for it" with a mechanical scan
that runs every time the suite runs.

THE RULE
--------
Scan every "shipping" .py module (see SHIPPING FILE SELECTION below) for
$-range literals ("$X - $Y" style, e.g. "$0.90 - $3.50") that appear within
+/-2 lines of a case-insensitive "cpc" or "cost-per-click" token. Every
such hit must be EITHER:
  (a) inside benchmark_registry.py (the canonical registry itself -- the
      thing everything else is supposed to single-source from), OR
  (b) covered by a row in tests/cpc_literal_ledger.json: an exact
      ``{"file": ..., "literal": ..., "status": "cited" | "estimate_disclosed"
      | "legacy_contained", "source": ...}`` object naming that exact file
      and that exact literal string.
A ledger row is keyed by (file, literal) only, not by line number -- one
row covers every occurrence of that literal string in that file (the same
figure often repeats across regions/verticals/duplicated tables; the point
is "is this dollar string accounted for," not "is this exact line
accounted for").

BOTH DIRECTIONS ARE ENFORCED, NOT JUST ONE
-------------------------------------------
1. Every scan hit needs a ledger row (or benchmark_registry.py membership)
   -- catches new uncited literals landing anywhere in shipping code.
2. Every ledger row needs a matching scan hit -- catches STALE rows: a
   literal that got refactored, deleted, or reworded so the ledger's claim
   about it no longer corresponds to anything in the actual source. A
   ledger that can silently drift out of sync with the code it describes
   is worse than no ledger (it would look like coverage while proving
   nothing) -- see the design-judge/fable-judgment precedent on this
   exact failure mode ("a green suite proves nothing here").

SHIPPING FILE SELECTION
------------------------
Included: every ``*.py`` file under the repo root EXCEPT the excluded
directories below. This picks up ``routes/*.py``, ``apis/**/*.py``, and
``archive/*.py`` along with every top-level module (app.py, nova.py,
ppt_generator.py, budget_engine.py, excel_v2.py, benchmark_registry.py,
...) -- anything not explicitly excluded is treated as shipping, on the
theory that "derive the exclusion list, don't hand-wave" means the default
should be INCLUSION, with every exclusion named and justified below, not
the other way around.

Excluded directories (every one named, per the task's explicit
requirement -- none of these are "shipping" in the sense this guard cares
about):
  - ``tests/``           -- the test suite itself (this file included).
  - ``docs/``             -- includes a few ``*.py`` files (e.g.
                             ``docs/generate_docx.py``,
                             ``docs/rag_implementation_sketch.py``) but
                             they are documentation/report generators, not
                             code the running product ships or serves.
  - ``scratchpad/``       -- does not exist in this repo today (checked:
                             zero ``*.py`` files under any ``scratchpad/``
                             path), named per the task's explicit
                             instruction so a future scratchpad directory
                             doesn't silently get scanned as shipping code.
  - ``.claude/``          -- does not exist in this repo today either (zero
                             ``*.py`` files), named for the same
                             future-proofing reason.
  - ``scripts/``          -- one-off tooling (data seeders, one-time audit
                             doc generators, migration helpers) run
                             manually by a developer, never imported by
                             app.py or any served request path.
  - ``.venv/``            -- third-party vendored dependencies, not this
                             repo's own code at all.
  - ``.git/``, ``__pycache__/`` -- not source.
  - ``.claude-flow/``, ``.claude-plugin/``, ``.github/``, ``.husky/``,
    ``.pytest_cache/`` -- tooling/CI directories; verified zero ``*.py``
    files under any of them today, named for the same future-proofing
    reason as scratchpad/.claude above.

ACCEPTED BLIND SPOTS (silence here is NOT coverage -- named explicitly so
nobody mistakes a clean run for "there are no more uncited CPC figures
anywhere," which this guard cannot claim)
------------------------------------------------------------------------
  - Bare-float dicts with no "$" sign at all (e.g. a raw ``2.60`` next to
    a ``"cpc"`` key) are invisible to this scanner -- it only matches the
    literal "$X - $Y" text shape. audit_tool.py's/performance_tracker.py's
    ``_FALLBACK_BENCHMARKS``/``_fallbacks`` dicts store their CPC value as
    a bare float (``"cpc": 2.60``) with the dollar-range citation living in
    an adjacent COMMENT, not the value itself -- caught here only because
    the comment itself contains a "$X-$Y" string; a bare float with no
    nearby dollar-range text at all would not be caught.
  - Non-dollar percentages, wage-per-hour figures without "cpc" nearby,
    and any other cost metric (CPA, CPH, CPM) that happens to be more than
    2 lines from a "cpc" token are invisible to this scanner even if
    uncited -- this guard is scoped to CPC specifically, per the task that
    created it, not a general "every dollar figure must be cited" audit.
  - Anything more than 2 lines from a "cpc"/"cost-per-click" token is
    invisible even if it IS a CPC figure -- e.g. archive/excel_legacy.py's
    ``peer_industries`` ALL-INDUSTRY-AVG row's CPH cell cites SHRM two
    cells over from an uncited CPC cell 3+ lines away; that CPC cell is
    covered here anyway (it's within the ledger's legacy_contained rows),
    but the general principle -- a citation-window miss is possible -- is
    a real, accepted limitation of a line-proximity heuristic, not
    something this guard can rule out for literals it never even flags as
    hits.
  - Literal-text matching only: this does not evaluate whether a "cited"
    status's SOURCE claim is actually true (that every dollar figure
    really does trace to the named KB path) -- that deeper verification is
    what the file-specific honesty-contract tests do (e.g.
    test_nova_platform_comparison_honesty.py,
    test_nova_channel_cpc_detail_honesty.py,
    test_ppt_benchmarks_fallback_honesty.py,
    test_excel_legacy_containment.py). This guard only enforces that every
    hit has SOME accounted-for status, not that the status is correct --
    that's why those file-specific tests still exist and are not replaced
    by this one.

Runs under pytest, or standalone: ``python3 tests/test_cpc_literal_ledger.py``.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Iterator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

LEDGER_PATH = Path(__file__).resolve().parent / "cpc_literal_ledger.json"

EXCLUDED_DIR_NAMES = {
    # named exclusions per task instruction (scratchpad/.claude don't exist
    # in this repo today; named anyway so they stay excluded if they ever
    # do)
    "tests",
    "docs",
    "scratchpad",
    ".claude",
    "scripts",
    # not this repo's own shipping code
    ".venv",
    ".git",
    "__pycache__",
    ".claude-flow",
    ".claude-plugin",
    ".github",
    ".husky",
    ".pytest_cache",
}

EXEMPT_FILE = "benchmark_registry.py"

DOLLAR_RANGE_RE = re.compile(
    r"\$[\d,]+(?:\.\d+)?\+?\s*(?:-|–|—|to)\s*\$[\d,]+(?:\.\d+)?\+?"
)
CPC_TOKEN_RE = re.compile(r"cpc|cost[\s_-]*per[\s_-]*click", re.IGNORECASE)

VALID_STATUSES = {"cited", "estimate_disclosed", "legacy_contained"}


def iter_shipping_py_files() -> Iterator[Path]:
    """Every *.py file under the repo root not inside an excluded dir."""
    for p in sorted(PROJECT_ROOT.rglob("*.py")):
        rel = p.relative_to(PROJECT_ROOT)
        if any(part in EXCLUDED_DIR_NAMES for part in rel.parts):
            continue
        yield rel


def scan_file_for_cpc_adjacent_dollar_ranges(rel_path: Path) -> list[tuple[int, str]]:
    """Return [(line_number, literal_text), ...] for every $-range literal
    within +/-2 lines of a cpc/cost-per-click token in this file."""
    text = (PROJECT_ROOT / rel_path).read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    hits = []
    for i, line in enumerate(lines):
        for m in DOLLAR_RANGE_RE.finditer(line):
            lo = max(0, i - 2)
            hi = min(len(lines), i + 3)
            window = "\n".join(lines[lo:hi])
            if CPC_TOKEN_RE.search(window):
                hits.append((i + 1, m.group(0)))
    return hits


def scan_all_shipping_files() -> dict[str, set[str]]:
    """{file: {distinct literal strings hit in that file}} across every
    shipping .py file, excluding benchmark_registry.py (structurally
    exempt, not ledger-tracked)."""
    result: dict[str, set[str]] = {}
    for rel in iter_shipping_py_files():
        rel_str = str(rel)
        if rel_str == EXEMPT_FILE:
            continue
        hits = scan_file_for_cpc_adjacent_dollar_ranges(rel)
        if hits:
            result[rel_str] = {literal for _lineno, literal in hits}
    return result


def load_ledger() -> list[dict]:
    with open(LEDGER_PATH, encoding="utf-8") as f:
        return json.load(f)


def test_ledger_rows_have_valid_shape_and_status():
    ledger = load_ledger()
    assert ledger, "ledger must not be empty"
    seen = set()
    for row in ledger:
        assert set(row.keys()) == {"file", "literal", "status", "source"}, (
            f"unexpected keys in ledger row: {row}"
        )
        assert row["status"] in VALID_STATUSES, (
            f"invalid status {row['status']!r} in row {row}; "
            f"must be one of {sorted(VALID_STATUSES)}"
        )
        assert row["source"].strip(), f"empty source in row {row}"
        key = (row["file"], row["literal"])
        assert key not in seen, f"duplicate ledger row for {key}"
        seen.add(key)


def test_every_cpc_adjacent_dollar_literal_has_ledger_coverage():
    """Direction 1: every scan hit is either in benchmark_registry.py or
    has a matching ledger row. A hit with neither is an uncited CPC
    literal that slipped past every prior per-file fix -- exactly the
    recurring defect this guard exists to catch mechanically."""
    scan_results = scan_all_shipping_files()
    ledger = load_ledger()
    ledger_pairs = {(row["file"], row["literal"]) for row in ledger}

    uncovered = []
    for file, literals in scan_results.items():
        for literal in literals:
            if (file, literal) not in ledger_pairs:
                uncovered.append((file, literal))

    assert not uncovered, (
        "uncited CPC-adjacent dollar literal(s) with no ledger row and not "
        "inside benchmark_registry.py -- either cite the figure and add a "
        "ledger row, or if it's a false positive (e.g. a non-CPC figure "
        "incidentally near a 'cpc' token), add a ledger row explaining "
        "why:\n"
        + "\n".join(f"  {f}: {lit!r}" for f, lit in sorted(uncovered))
    )


def test_every_ledger_row_still_matches_real_code():
    """Direction 2 (STALE check): every ledger row's (file, literal) pair
    must still appear in that file's current scan hits. A row whose
    literal no longer appears anywhere -- because the code was refactored,
    the figure changed, or the line was deleted -- is a stale claim about
    code that no longer exists, and must be removed or updated rather than
    left to accumulate as false coverage."""
    scan_results = scan_all_shipping_files()
    ledger = load_ledger()

    stale = []
    for row in ledger:
        file, literal = row["file"], row["literal"]
        if literal not in scan_results.get(file, set()):
            stale.append((file, literal))

    assert not stale, (
        "stale ledger row(s) -- (file, literal) no longer matches any "
        "current CPC-adjacent dollar literal in that file. Either the "
        "code changed (update or remove the row) or the row was "
        "mistyped:\n"
        + "\n".join(f"  {f}: {lit!r}" for f, lit in sorted(stale))
    )


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
