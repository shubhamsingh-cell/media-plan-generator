"""AST lint: ``X or <default> <op> Y`` operator-precedence bug class (audit D-09).

The bug class
-------------
``or`` binds looser than every arithmetic and comparison operator, so

    now - v.get("created_at") or 0 > TTL

parses as ``(now - v.get("created_at")) or (0 > TTL)`` -- NOT as the intended
``(now - (v.get("created_at") or 0)) > TTL``. The left side is a nonzero number
(truthy) for every real value, so the whole predicate is always true. The
shipped instance deleted every shared plan and every async plan result on
every cache-cleanup sweep (see tests/test_cache_cleanup_ttl.py). The same slip
with a string default -- ``row.get("own_code") or "" == "5"`` -- parses as
``row.get("own_code") or ("" == "5")`` and matches ANY truthy own_code.

What this lint flags
--------------------
In every non-test, non-archive, non-.claude ``*.py`` file, an ``ast.BoolOp``
with operator ``or`` that has an operand (after the first)

  * that is an ``ast.Compare`` or ``ast.BinOp`` whose leftmost operand is a
    DEFAULT LITERAL: numeric ``0`` / ``0.0``, ``""`` / ``b""``, or an empty
    ``[]`` / ``()`` / ``{}`` (``... or 0 > y``, ``... or "" == y``,
    ``... or 0 + y``), and
  * that is preceded, in the same ``or`` chain, by an operand that is an
    ``ast.BinOp``, ``Call``, ``Attribute``, ``Subscript``, ``BoolOp`` (an
    ``and``/``or`` chain: ``a and b.get(k) or 0 > 20``), ``UnaryOp`` or
    ``NamedExpr`` -- the author was almost certainly defaulting a value with
    ``or <default>`` and forgot the parentheses around ``(X or <default>)``.

Intentional chained ranges are NOT flagged. ``self.enabled or 0 < n <= MAX`` is
a deliberate range test: a ``Compare`` with more than one comparator whose left
side is the literal reads as ``0 < n <= MAX`` and has no sensible
``(X or 0) < n <= MAX`` reading. Only a single-comparator compare
(``X or 0 > y``) is ambiguous enough to flag.

The plain default idiom ``x or 0`` is NOT flagged (its last operand is a bare
Constant), nor is ``(x or 0) > y`` (the BoolOp is parenthesised, so it is not an
operand of an outer chain).

Sibling shape (also fails the gate): an arithmetic ``ast.BinOp`` immediately
followed, in the same ``or`` chain, by the numeric literal ``0`` -- e.g.
``time.time() - state.get("t") or 0``. Defaulting the RESULT of arithmetic to
0 is a no-op (``x - y`` is already 0 when it is 0), so the author meant to
default an operand: ``time.time() - (state.get("t") or 0)``. As written, a
None operand raises ``TypeError`` instead of being defaulted. (A non-zero
fallback such as ``total - used or 1`` is a deliberate divide-by-zero guard and
is not flagged; nor is a string default on concatenation.)

Scope and speed
---------------
The file list comes from ``git ls-files '*.py'`` (plus untracked-but-not-ignored
files, so a brand-new module is linted before its first commit), with an
``os.walk`` fallback when git is unavailable. Every file is parsed ONCE per
session (``_scan_repo_cached``) and all tests share the result.

An unparsable / non-UTF-8 file is reported, never silently dropped: for a
TRACKED file (or any file in the walk fallback) it fails the gate; for an
UNTRACKED file it is listed as skipped-with-reason.

Allowlist
---------
``ALLOWLIST`` maps ``(relative path, unparsed BoolOp text)`` to the reason the
hit was verified as intentional. Keyed on text rather than line number so an
unrelated edit above the site does not silently invalidate the entry.
"""

from __future__ import annotations

import ast
import functools
import os
import subprocess
from pathlib import Path
from typing import Iterable, Iterator, NamedTuple

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Directory names never scanned (tests intentionally contain planted defects;
# archive/ is dead code; .claude/ holds worktrees and tooling copies).
_EXCLUDED_DIRS = frozenset(
    {
        "tests",
        "archive",
        ".claude",
        ".git",
        "node_modules",
        "__pycache__",
        "venv",
        ".venv",
        "site-packages",
    }
)

# (relative posix path, ast.unparse(BoolOp)) -> reason verified intentional.
ALLOWLIST: dict[tuple[str, str], str] = {}

_SUSPECT_EARLIER = (
    ast.BinOp,
    ast.Call,
    ast.Attribute,
    ast.Subscript,
    ast.BoolOp,
    ast.UnaryOp,
    ast.NamedExpr,
)


class Hit(NamedTuple):
    path: str
    line: int
    text: str


class Skipped(NamedTuple):
    path: str
    reason: str


class ScanResult(NamedTuple):
    hits: list[Hit]
    scanned: int
    failed: list[Skipped]  # tracked (or walk-fallback) files we could not parse
    skipped: list[Skipped]  # untracked files we could not parse


def _is_zero_literal(node: ast.AST) -> bool:
    """True for the numeric constants ``0`` and ``0.0`` (never ``False``)."""
    return (
        isinstance(node, ast.Constant)
        and isinstance(node.value, (int, float))
        and not isinstance(node.value, bool)
        and node.value == 0
    )


def _is_default_literal(node: ast.AST) -> bool:
    """``0`` / ``0.0`` / ``""`` / ``b""`` / ``[]`` / ``()`` / ``{}``."""
    if _is_zero_literal(node):
        return True
    if isinstance(node, ast.Constant):
        return isinstance(node.value, (str, bytes)) and len(node.value) == 0
    if isinstance(node, (ast.List, ast.Tuple)):
        return not node.elts
    if isinstance(node, ast.Dict):
        return not node.keys
    return False


def _leftmost_operand(node: ast.AST) -> ast.AST:
    """Descend ``BinOp.left`` / ``Compare.left`` to the leftmost leaf."""
    while isinstance(node, (ast.BinOp, ast.Compare)):
        node = node.left
    return node


def _or_chains(tree: ast.AST) -> Iterator[ast.BoolOp]:
    for node in ast.walk(tree):
        if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
            yield node


def _is_default_then_expr_bug(node: ast.BoolOp) -> bool:
    """Shape 1: ``<suspect> or <default> <op> Y`` (a later operand led by a
    default literal). A chained compare (``0 < n <= MAX``) is an intentional
    range and is skipped."""
    for i, operand in enumerate(node.values):
        if i == 0:
            continue
        if isinstance(operand, ast.Compare):
            if len(operand.comparators) > 1:
                continue  # chained range: `flag or 0 < n <= MAX`
        elif not isinstance(operand, ast.BinOp):
            continue
        if not _is_default_literal(_leftmost_operand(operand)):
            continue
        if any(isinstance(e, _SUSPECT_EARLIER) for e in node.values[:i]):
            return True
    return False


def _is_arith_then_or_zero_bug(node: ast.BoolOp) -> bool:
    """Shape 2 (sibling): an arithmetic BinOp directly followed by literal 0."""
    return any(
        isinstance(cur, ast.BinOp) and _is_zero_literal(nxt)
        for cur, nxt in zip(node.values, node.values[1:])
    )


def find_precedence_hits(source: str, path: str = "<src>") -> list[Hit]:
    """Return both precedence-bug shapes found in ``source`` (empty if clean)."""
    nodes = [
        n
        for n in _or_chains(ast.parse(source))
        if _is_default_then_expr_bug(n) or _is_arith_then_or_zero_bug(n)
    ]
    return [
        Hit(path, n.lineno, ast.unparse(n))
        for n in sorted(nodes, key=lambda n: (n.lineno, n.col_offset))
    ]


# ── File discovery ──────────────────────────────────────────────────────────


def _in_scope(rel: str) -> bool:
    parts = Path(rel).parts
    return rel.endswith(".py") and not any(p in _EXCLUDED_DIRS for p in parts[:-1])


def _git_ls(root: Path, *extra: str) -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", *extra, "--", "*.py"],
        capture_output=True,
        check=True,
        timeout=30,
    ).stdout
    return [os.fsdecode(p) for p in out.split(b"\0") if p]


def list_python_files(root: Path = PROJECT_ROOT) -> tuple[list[str], list[str]]:
    """Return ``(tracked, untracked)`` in-scope ``*.py`` paths relative to root.

    Uses git when available. Without git (or outside a repo) falls back to an
    ``os.walk`` and treats every file as tracked, so an unparsable file still
    fails the gate in that environment.
    """
    try:
        tracked = _git_ls(root)
        untracked = _git_ls(root, "--others", "--exclude-standard")
    except (OSError, subprocess.SubprocessError):
        walked: list[str] = []
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = sorted(d for d in dirnames if d not in _EXCLUDED_DIRS)
            for name in sorted(filenames):
                if name.endswith(".py"):
                    walked.append((Path(dirpath) / name).relative_to(root).as_posix())
        return walked, []
    return (
        sorted(p for p in tracked if _in_scope(p)),
        sorted(p for p in untracked if _in_scope(p)),
    )


def scan_files(
    root: Path, tracked: Iterable[str], untracked: Iterable[str] = ()
) -> ScanResult:
    """Parse each file once and collect hits / unparsable files."""
    hits: list[Hit] = []
    failed: list[Skipped] = []
    skipped: list[Skipped] = []
    scanned = 0
    for is_tracked, rels in ((True, tracked), (False, untracked)):
        for rel in rels:
            try:
                source = (root / rel).read_text(encoding="utf-8")
            except FileNotFoundError:
                continue  # tracked file deleted in the working tree
            except (OSError, UnicodeDecodeError) as exc:
                entry = Skipped(rel, f"unreadable: {type(exc).__name__}: {exc}")
                (failed if is_tracked else skipped).append(entry)
                continue
            try:
                found = find_precedence_hits(source, rel)
            except (SyntaxError, ValueError) as exc:
                entry = Skipped(rel, f"unparsable: {type(exc).__name__}: {exc}")
                (failed if is_tracked else skipped).append(entry)
                continue
            scanned += 1
            hits.extend(found)
    return ScanResult(hits, scanned, failed, skipped)


@functools.lru_cache(maxsize=1)
def _scan_repo_cached() -> ScanResult:
    """Scan the repo once per session; every test below shares the result."""
    tracked, untracked = list_python_files(PROJECT_ROOT)
    return scan_files(PROJECT_ROOT, tracked, untracked)


# ── Scanner self-tests (planted defects) ────────────────────────────────────


class TestScannerDetectsPlantedDefects:
    """The lint is only worth trusting if it fires on the real bug shapes."""

    @pytest.mark.parametrize(
        "src",
        [
            # The two shipped instances, verbatim.
            'x = [k for k, v in d.items() if now - v.get("created_at") or 0 > TTL]',
            'x = [k for k, v in d.items() if now - v.get("created") or 0 > TTL]',
            # Call / Attribute / Subscript defaulted with a missing paren.
            'ok = cfg.get("limit") or 0 > threshold',
            "ok = obj.count or 0 > threshold",
            "ok = rows[0] or 0 > threshold",
            # BinOp-last variants: author meant (X or 0) + Y.
            'total = cfg.get("a") or 0 + extra',
            "total = obj.base or 0.0 * rate",
            # Longer chains and nested leftmost literal.
            'ok = a.b or c.get("x") or 0 > y',
            'total = cfg.get("a") or 0 + extra + more',
            # Compare whose left side is itself a BinOp led by 0.
            'ok = cfg.get("a") or 0 + extra > limit',
            # Inside a conditional expression test.
            'v = 1 if now - e.get("t") or 0 > ttl else 2',
            # Two defaults with an addition between them (Bing min/max average).
            'avg = (lo.get("cpc") or 0 + hi.get("cpc") or 0) / 2.0',
            'total = a.get("x") or 0 + b.get("y") or 0 + c.get("z") or 0',
            # Sibling shape: arithmetic result defaulted to 0 (no-op default).
            'elapsed = time.time() - state.get("last_failure_time") or 0',
            'age = (now - jdata.get("created") or 0) > 600',
            "n = a.b + c.d or 0",
            "n = x or a.b * c or 0",
            # `and` chain as the earlier operand (hire_signal funnel bug).
            'if a > 0 and b < 1.0 and src.get("total_applications") or 0 > 20: pass',
            # String default (the six `or ""` bugs).
            'if ch.get("category") or "" == cat: pass',
            'if area == "US000" and row.get("own_code") or "" == "5": pass',
            'keep = [n for n in xs if n.get("name") or "" != entry["name"]]',
            'if q["status"] == "pending" and q.get("ts") or "" >= week_ago: pass',
            'label = (l.get("city") or "" + ", " + l.get("state") or "")',
            # Empty-container defaults.
            'v = cfg.get("a") or [] + extra',
            'v = cfg.get("a") or {} == other',
            'v = cfg.get("a") or () + extra',
            # UnaryOp / NamedExpr as the earlier operand.
            "ok = not a.b or 0 > y",
            "ok = -a.b or 0 > y",
            "ok = (n := f()) or 0 > y",
        ],
    )
    def test_flags_bug_shape(self, src: str) -> None:
        assert find_precedence_hits(src), f"lint missed planted defect: {src}"

    @pytest.mark.parametrize(
        "src",
        [
            # The corrected forms.
            'x = (now - (v.get("created_at") or 0)) > TTL',
            'ok = (cfg.get("limit") or 0) > threshold',
            'total = (cfg.get("a") or 0) + extra',
            'if (ch.get("category") or "") == cat: pass',
            'if area == "US000" and (row.get("own_code") or "") == "5": pass',
            'if a > 0 and b < 1.0 and (src.get("total_applications") or 0) > 20: pass',
            # Plain default idiom.
            'n = cfg.get("a") or 0',
            "n = obj.count or 0.0",
            'v = cfg.get("a") or ""',
            'v = cfg.get("a") or []',
            'v = cfg.get("a") or {}',
            # Last operand has a non-zero / non-literal left side.
            "ok = a.b or c > 0",
            'ok = cfg.get("a") or y > 0',
            "ok = a.b or 1 > y",
            'ok = cfg.get("a") or "x" == y',
            # 0 on the left but no suspect earlier operand (bare names).
            "ok = flag or 0 > y",
            # `and` chains are not this bug.
            'ok = cfg.get("a") and 0 > y',
            # Boolean False is not the numeric 0.
            'ok = cfg.get("a") or False > y',
            # Corrected two-default sum.
            'avg = ((lo.get("cpc") or 0) + (hi.get("cpc") or 0)) / 2.0',
            # Corrected sibling forms.
            'elapsed = time.time() - (state.get("last_failure_time") or 0)',
            'age = (now - (jdata.get("created") or 0)) > 600',
            # A non-zero fallback on arithmetic is a deliberate guard.
            "ratio = n / (total - used or 1)",
            "x = total - used or 1",
            # A string default on concatenation is not the numeric-zero shape.
            'label = a + b or ""',
        ],
    )
    def test_does_not_flag_good_shape(self, src: str) -> None:
        assert find_precedence_hits(src) == [], f"false positive on: {src}"

    @pytest.mark.parametrize(
        "src",
        [
            # Intentional chained ranges: `0 < n <= MAX` is one comparison.
            "ok = self.enabled or 0 < n <= MAX",
            "ok = cfg.get('on') or 0 < n < limit",
            "ok = obj.attr or 0 <= idx < len(items)",
            "ok = not flag.value or 0 < n <= MAX",
            'ok = a.b or "" < s <= t',
            "ok = a.b or 0 + offset < n <= MAX",
        ],
    )
    def test_chained_range_is_not_flagged(self, src: str) -> None:
        assert find_precedence_hits(src) == [], f"false positive on range: {src}"

    def test_single_comparator_with_default_left_is_still_flagged(self) -> None:
        """`X or 0 < n` has a sensible `(X or 0) < n` reading, unlike the chain."""
        assert find_precedence_hits("ok = self.enabled or 0 < n")

    def test_hit_reports_line_and_text(self) -> None:
        (hit,) = find_precedence_hits("a = 1\nb = x.get('k') or 0 > 5\n", "m.py")
        assert (hit.path, hit.line) == ("m.py", 2)
        assert hit.text == "x.get('k') or 0 > 5"


class TestFileDiscovery:
    def test_tracked_unparsable_fails_untracked_unparsable_is_skipped(
        self, tmp_path: Path
    ) -> None:
        (tmp_path / "ok.py").write_text("x = 1\n", encoding="utf-8")
        (tmp_path / "broken_tracked.py").write_text("def (:\n", encoding="utf-8")
        (tmp_path / "broken_untracked.py").write_text("def (:\n", encoding="utf-8")
        (tmp_path / "latin_untracked.py").write_bytes(b"x = '\xe9'\n")
        (tmp_path / "latin_tracked.py").write_bytes(b"x = '\xe9'\n")
        result = scan_files(
            tmp_path,
            tracked=["ok.py", "broken_tracked.py", "latin_tracked.py"],
            untracked=["broken_untracked.py", "latin_untracked.py"],
        )
        assert result.scanned == 1
        assert sorted(s.path for s in result.failed) == [
            "broken_tracked.py",
            "latin_tracked.py",
        ]
        assert sorted(s.path for s in result.skipped) == [
            "broken_untracked.py",
            "latin_untracked.py",
        ]
        reasons = {s.path: s.reason for s in result.failed + result.skipped}
        assert reasons["broken_tracked.py"].startswith("unparsable: SyntaxError")
        assert reasons["latin_untracked.py"].startswith(
            "unreadable: UnicodeDecodeError"
        )

    def test_tracked_file_deleted_from_working_tree_is_ignored(
        self, tmp_path: Path
    ) -> None:
        result = scan_files(tmp_path, tracked=["gone.py"])
        assert (result.scanned, result.failed, result.skipped) == (0, [], [])

    def test_walk_fallback_when_git_unavailable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "pkg").mkdir()
        (tmp_path / "pkg" / "a.py").write_text("x = 1\n", encoding="utf-8")
        (tmp_path / "tests").mkdir()
        (tmp_path / "tests" / "t.py").write_text("x = 1\n", encoding="utf-8")
        (tmp_path / "archive").mkdir()
        (tmp_path / "archive" / "old.py").write_text("x = 1\n", encoding="utf-8")
        (tmp_path / "top.py").write_text("x = 1\n", encoding="utf-8")

        def _no_git(*args: object, **kwargs: object) -> None:
            raise FileNotFoundError("git")

        monkeypatch.setattr(subprocess, "run", _no_git)
        tracked, untracked = list_python_files(tmp_path)
        assert sorted(tracked) == ["pkg/a.py", "top.py"]
        assert untracked == []

    def test_git_listing_excludes_out_of_scope_dirs(self) -> None:
        tracked, untracked = list_python_files()
        everything = tracked + untracked
        assert "app.py" in everything
        assert not any(
            p.split("/")[0] in {"tests", "archive", ".claude"} for p in everything
        )


# ── The repo gate ───────────────────────────────────────────────────────────


class TestRepoIsFreeOfPrecedenceBug:
    def test_walker_is_not_vacuous(self) -> None:
        """Guard against an exclusion bug silently scanning nothing."""
        result = _scan_repo_cached()
        tracked, _untracked = list_python_files()
        assert result.scanned >= 100
        assert "app.py" in tracked
        assert "routes/campaign.py" in tracked

    def test_scan_is_computed_once_per_session(self) -> None:
        assert _scan_repo_cached() is _scan_repo_cached()

    def test_no_unparsable_tracked_source_files(self) -> None:
        """An unparsable tracked file would be invisible to the lint."""
        failed = _scan_repo_cached().failed
        assert failed == [], "files the lint could not parse:\n" + "\n".join(
            f"  {s.path}: {s.reason}" for s in failed
        )

    def test_untracked_unparsable_files_are_reported_not_silent(self) -> None:
        """Skipped entries are allowed but must always carry a reason."""
        for s in _scan_repo_cached().skipped:
            assert s.reason, f"skipped without a reason: {s.path}"

    def test_no_or_default_precedence_bugs(self) -> None:
        offenders = [
            h for h in _scan_repo_cached().hits if (h.path, h.text) not in ALLOWLIST
        ]
        assert not offenders, (
            "`X or <default> <op> Y` parses as `X or (<default> <op> Y)`. "
            "Parenthesise the default -- `(X or <default>) <op> Y` -- or, if "
            "verified intentional, add the (path, text) pair to ALLOWLIST with "
            "a reason:\n"
            + "\n".join(f"  {h.path}:{h.line}: {h.text}" for h in offenders)
        )

    def test_allowlist_entries_are_still_live(self) -> None:
        """A stale allowlist entry would hide a future regression at that text."""
        live = {(h.path, h.text) for h in _scan_repo_cached().hits}
        stale = sorted(set(ALLOWLIST) - live)
        assert not stale, f"ALLOWLIST entries no longer match any hit: {stale}"

    def test_allowlist_entries_carry_a_reason(self) -> None:
        for key, reason in ALLOWLIST.items():
            assert reason.strip(), f"ALLOWLIST entry without a reason: {key}"
