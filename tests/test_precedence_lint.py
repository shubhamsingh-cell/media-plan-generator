"""AST lint: ``X or 0 <op> Y`` operator-precedence bug class (audit item D-09).

The bug class
-------------
``or`` binds looser than every arithmetic and comparison operator, so

    now - v.get("created_at") or 0 > TTL

parses as ``(now - v.get("created_at")) or (0 > TTL)`` -- NOT as the intended
``(now - (v.get("created_at") or 0)) > TTL``. The left side is a nonzero number
(truthy) for every real value, so the whole predicate is always true. The
shipped instance deleted every shared plan and every async plan result on
every cache-cleanup sweep (see tests/test_cache_cleanup_ttl.py).

What this lint flags
--------------------
In every non-test, non-archive, non-.claude ``*.py`` file, an ``ast.BoolOp``
with operator ``or`` that has an operand (after the first; in practice the last,
or a middle one in ``a.get() or 0 + b.get() or 0``)

  * that is an ``ast.Compare`` or ``ast.BinOp`` whose leftmost operand is the
    numeric literal ``0`` / ``0.0``  (``... or 0 > y``, ``... or 0 + y``), and
  * that is preceded, in the same ``or`` chain, by an operand that is an
    ``ast.BinOp``, ``ast.Call``, ``ast.Attribute`` or ``ast.Subscript`` (the
    author was almost certainly defaulting a value with ``or 0`` and forgot
    the parentheses around ``(X or 0)``).

The plain default idiom ``x or 0`` is NOT flagged (its last operand is a bare
Constant), nor is ``(x or 0) > y`` (the BoolOp is parenthesised, so it is not
the last operand of an outer chain).

Sibling shape (also fails the gate): an arithmetic ``ast.BinOp`` immediately
followed, in the same ``or`` chain, by the numeric literal ``0`` -- e.g.
``time.time() - state.get("t") or 0``. Defaulting the RESULT of arithmetic to
0 is a no-op (``x - y`` is already 0 when it is 0), so the author meant to
default an operand: ``time.time() - (state.get("t") or 0)``. As written, a
None operand raises ``TypeError`` instead of being defaulted. (A non-zero
fallback such as ``total - used or 1`` is a deliberate divide-by-zero guard and
is not flagged.)

Allowlist
---------
``ALLOWLIST`` maps ``(relative path, unparsed BoolOp text)`` to the reason the
hit was verified as intentional. Keyed on text rather than line number so an
unrelated edit above the site does not silently invalidate the entry.
"""

from __future__ import annotations

import ast
import os
from pathlib import Path
from typing import Iterator, NamedTuple

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

_SUSPECT_EARLIER = (ast.BinOp, ast.Call, ast.Attribute, ast.Subscript)


class Hit(NamedTuple):
    path: str
    line: int
    text: str


def _is_zero_literal(node: ast.AST) -> bool:
    """True for the numeric constants ``0`` and ``0.0`` (never ``False``)."""
    return (
        isinstance(node, ast.Constant)
        and isinstance(node.value, (int, float))
        and not isinstance(node.value, bool)
        and node.value == 0
    )


def _leftmost_operand(node: ast.AST) -> ast.AST:
    """Descend ``BinOp.left`` / ``Compare.left`` to the leftmost leaf."""
    while isinstance(node, (ast.BinOp, ast.Compare)):
        node = node.left
    return node


def _or_chains(tree: ast.AST) -> Iterator[ast.BoolOp]:
    for node in ast.walk(tree):
        if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
            yield node


def _is_or_zero_then_expr_bug(node: ast.BoolOp) -> bool:
    """Shape 1: ``<suspect> or 0 <op> Y`` (a later operand led by literal 0)."""
    return any(
        isinstance(operand, (ast.Compare, ast.BinOp))
        and _is_zero_literal(_leftmost_operand(operand))
        and any(isinstance(e, _SUSPECT_EARLIER) for e in node.values[:i])
        for i, operand in enumerate(node.values)
        if i > 0
    )


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
        if _is_or_zero_then_expr_bug(n) or _is_arith_then_or_zero_bug(n)
    ]
    return [
        Hit(path, n.lineno, ast.unparse(n))
        for n in sorted(nodes, key=lambda n: (n.lineno, n.col_offset))
    ]


def iter_source_files(root: Path = PROJECT_ROOT) -> Iterator[Path]:
    """Yield every in-scope ``*.py`` file under ``root``."""
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in _EXCLUDED_DIRS)
        for name in sorted(filenames):
            if name.endswith(".py"):
                yield Path(dirpath) / name


def scan_repo(root: Path = PROJECT_ROOT) -> tuple[list[Hit], int, list[str]]:
    """Scan the repo. Returns ``(hits, files_scanned, unparsable_paths)``."""
    hits: list[Hit] = []
    scanned = 0
    unparsable: list[str] = []
    for path in iter_source_files(root):
        rel = path.relative_to(root).as_posix()
        try:
            source = path.read_text(encoding="utf-8")
            found = find_precedence_hits(source, rel)
        except (SyntaxError, UnicodeDecodeError, ValueError):
            unparsable.append(rel)
            continue
        scanned += 1
        hits.extend(found)
    return hits, scanned, unparsable


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
            # Plain default idiom.
            'n = cfg.get("a") or 0',
            "n = obj.count or 0.0",
            # Last operand has a non-zero / non-literal left side.
            "ok = a.b or c > 0",
            'ok = cfg.get("a") or y > 0',
            "ok = a.b or 1 > y",
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


# ── The repo gate ───────────────────────────────────────────────────────────


class TestRepoIsFreeOfPrecedenceBug:
    def test_walker_is_not_vacuous(self) -> None:
        """Guard against an exclusion bug silently scanning nothing."""
        _hits, scanned, _unparsable = scan_repo()
        rels = {p.relative_to(PROJECT_ROOT).as_posix() for p in iter_source_files()}
        assert scanned >= 100
        assert "app.py" in rels
        assert "routes/campaign.py" in rels
        assert not any(r.startswith(("tests/", "archive/", ".claude/")) for r in rels)

    def test_no_unparsable_source_files(self) -> None:
        """An unparsable file would be invisible to the lint."""
        _hits, _scanned, unparsable = scan_repo()
        assert unparsable == [], f"files the lint could not parse: {unparsable}"

    def test_no_or_zero_precedence_bugs(self) -> None:
        hits, _scanned, _unparsable = scan_repo()
        offenders = [h for h in hits if (h.path, h.text) not in ALLOWLIST]
        assert not offenders, (
            "`X or 0 <op> Y` parses as `X or (0 <op> Y)`. Parenthesise the "
            "default -- `(X or 0) <op> Y` -- or, if verified intentional, add "
            "the (path, text) pair to ALLOWLIST with a reason:\n"
            + "\n".join(f"  {h.path}:{h.line}: {h.text}" for h in offenders)
        )

    def test_allowlist_entries_are_still_live(self) -> None:
        """A stale allowlist entry would hide a future regression at that text."""
        hits, _scanned, _unparsable = scan_repo()
        live = {(h.path, h.text) for h in hits}
        stale = sorted(set(ALLOWLIST) - live)
        assert not stale, f"ALLOWLIST entries no longer match any hit: {stale}"
