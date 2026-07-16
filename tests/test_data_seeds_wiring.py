"""Source-guard regression test for app.py's data-seed wiring (38ac22a).

Background: 38ac22a fixed prod's cold-start data gap by calling
``data_seeds.seed_runtime_data_files()`` at app.py IMPORT time (module
scope), before anything reads the runtime data files it seeds. Two ways
this wiring can silently regress while every other test stays green,
because ``budget_engine`` / ``channel_recommender`` / ``nova`` / ``excel_v2``
are imported directly by their own test modules rather than through
app.py's import order:

    1. Someone removes (or comments out, or accidentally de-indents into a
       conditional) the module-scope ``data_seeds.seed_runtime_data_files()``
       call in app.py. Every test that imports ``budget_engine`` etc.
       directly still passes, since it never goes through app.py's import
       order -- only a fresh prod cold start would notice, as a silent
       reversion to the pre-38ac22a live_benchmark-tier-missing bug.
    2. A future change adds a module-scope
       ``import budget_engine`` / ``channel_recommender`` / ``nova`` /
       ``excel_v2`` (or ``from <module> import ...``) ABOVE the seed call.
       Those readers cache their file reads at first call / first import,
       so importing one before seeding runs would bake in the "no live
       file yet" empty read for the app's entire process lifetime, same
       failure shape as (1).

This mirrors the source-inspection pattern used elsewhere in this suite for
app.py wiring that isn't independently unit-testable (see
tests/test_app_duration_wiring.py, tests/test_app_vendor_gate_wiring.py):
parse app.py's source text directly rather than trying to unit-test a
~26k-line request-handler module in isolation.

Runs under pytest, or standalone:
``python3 tests/test_data_seeds_wiring.py``.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

APP_PY_PATH = PROJECT_ROOT / "app.py"

# The seed call must appear at module scope (column 0) -- an indented call
# would be inside some conditional/function and might never run at import
# time.
_SEED_CALL_RE = re.compile(
    r"^data_seeds\.seed_runtime_data_files\(\)\s*$", re.MULTILINE
)

# Module-scope (column-0, i.e. non-indented) imports of the readers that
# consume the seeded files. Word-boundaried so e.g. "from nova_cache import"
# or "import nova_persistence" (both real, legitimate app.py imports) don't
# false-positive.
_READER_IMPORT_RE = re.compile(
    r"^(?:"
    r"import\s+(budget_engine|channel_recommender|nova|excel_v2)\b"
    r"|from\s+(budget_engine|channel_recommender|nova|excel_v2)\s+import\b"
    r")",
    re.MULTILINE,
)


def _seed_call_offset(src: str) -> int:
    """Character offset of the module-scope seed call, or -1 if absent."""
    m = _SEED_CALL_RE.search(src)
    return m.start() if m else -1


def _module_scope_reader_imports(src: str) -> list[tuple[str, int]]:
    """(module_name, char_offset) for every module-scope import of a reader
    module, in source order."""
    out = []
    for m in _READER_IMPORT_RE.finditer(src):
        name = m.group(1) or m.group(2)
        out.append((name, m.start()))
    return out


def test_seed_call_exists_at_module_scope():
    src = APP_PY_PATH.read_text(encoding="utf-8")
    offset = _seed_call_offset(src)
    assert offset != -1, (
        "app.py no longer calls data_seeds.seed_runtime_data_files() at "
        "module scope -- prod cold starts would silently drop the "
        "live_benchmark CPC tier again (the exact bug 38ac22a fixed), and "
        "no existing test would catch it since readers are imported "
        "directly rather than through app.py's import order"
    )


def test_no_reader_module_imported_at_module_scope_before_seed_call():
    src = APP_PY_PATH.read_text(encoding="utf-8")
    seed_offset = _seed_call_offset(src)
    assert seed_offset != -1, (
        "cannot check import ordering -- module-scope seed call not found "
        "(see test_seed_call_exists_at_module_scope)"
    )

    for name, imp_offset in _module_scope_reader_imports(src):
        assert imp_offset > seed_offset, (
            f"app.py imports '{name}' at module scope (offset {imp_offset}) "
            f"BEFORE data_seeds.seed_runtime_data_files() runs (offset "
            f"{seed_offset}). {name} caches its file read on first use, so "
            f"importing it before seeding would bake in an empty read for "
            f"the app's entire process lifetime -- move the import below "
            f"the seed call, or the seed call above the import."
        )


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
