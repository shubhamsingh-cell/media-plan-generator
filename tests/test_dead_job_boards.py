"""No shipped surface may name a defunct or misdescribed job board.

- NurseFly became Vivian Health in 2021 (list Vivian Health instead, never both).
- Stack Overflow Jobs shut 2022-03-31; GitHub Jobs shut 2021 -- neither is a
  job board any more, and a bare "Stack Overflow" is not one either.
- SeniorJobBank.com is a job bank for job seekers aged 50+, not a
  senior-living industry board.

The Stack Overflow *Developer Survey* is a research data source (not a job
board) and is allowlisted by file below. Scans git-tracked source, data and
templates; archive/, tests/, docs/ and other non-shipped trees are skipped.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

_PATTERN = re.compile(
    r"nursefly|stack\s*overflow|github\s+jobs|seniorjobbank|senior\s+job\s+bank",
    re.IGNORECASE,
)
# Historical client media-plan records (what a past client's plan actually
# bought, e.g. RTX's Stack Overflow ads line) are source records, not board
# recommendations, and are not rewritten.
_HISTORICAL_CLIENT_RECORDS = {
    "data/client_media_plans_kb.json",
    "data/client_plans/rtx_aerospace_defense_benchmarks.json",
    "data/client_plans/rtx_usa_media_plan.json",
}

_SKIP_PREFIXES = (
    "archive/",
    "tests/",
    "docs/",
    ".claude/",
    "ledger/",
    "rpi/",
    "graphify-out/",
)
# Stack Overflow Developer Survey data source (not a job board).
_SURVEY_FILES = {
    ".gitignore",
    "apis/data/so_survey_consumer.py",
    "apis/data/stack_overflow_survey.py",
    "scripts/aggregate_so_survey_2025.py",
    "data/so_survey_2025_aggregates.json",
}
_BINARY_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".ico", ".woff", ".woff2", ".pdf", ".xlsx", ".pptx", ".zip", ".pyc"}


def _tracked_files():
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=ROOT, capture_output=True, check=True
    ).stdout.decode("utf-8", "replace")
    for rel in filter(None, out.split("\0")):
        if (
            rel.startswith(_SKIP_PREFIXES)
            or rel in _SURVEY_FILES
            or rel in _HISTORICAL_CLIENT_RECORDS
        ):
            continue
        if Path(rel).suffix.lower() in _BINARY_SUFFIXES:
            continue
        yield rel


def test_no_dead_or_misdescribed_job_boards_in_shipped_files():
    hits = []
    for rel in _tracked_files():
        try:
            text = (ROOT / rel).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            if _PATTERN.search(line):
                hits.append(f"{rel}:{lineno}: {line.strip()[:110]}")
    assert not hits, "dead/misdescribed job boards still named:\n" + "\n".join(hits)
