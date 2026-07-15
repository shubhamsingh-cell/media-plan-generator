"""Regression test for a prod-verified async-only crash (build
4.0.0-c6cafddb, 2026-07-15): ``POST /api/generate`` with header
``X-Async: true`` and roles as list-of-dicts (e.g.
``[{"title": "CDL A Driver", "count": 250}]``) reached terminal
``status=failed`` with error ``sequence item 0: expected str instance,
dict found``. The synchronous path (no ``X-Async``) completed the
identical brief -- dict-shaped roles are a documented/allowed input shape.

Root cause: ``classify_industry()`` (app.py) builds
``f"{raw_industry} {company_name} {' '.join(roles or [])}"`` assuming
``roles: list[str]``. The synchronous ``/api/generate`` handler already
normalizes dict-shaped roles to title strings (``_normalize_dict_roles``)
well before its own ``classify_industry()`` call. The async worker
closure (``_run_async_generate``, spun onto a background thread by the
``X-Async`` branch) calls ``classify_industry()`` on its own, much
earlier in its independently-ordered pipeline, and previously had no
normalization step at all -- so it received the raw dict-shaped roles
list and crashed.

Fix: extracted the sync handler's existing isinstance-branch into a
single module-level helper, ``app._normalize_dict_roles``, called
identically (same function, not a duplicated copy) from both the sync
handler and the top of ``_run_async_generate``.

This file covers:
    1. Unit -- ``_normalize_dict_roles`` flattens dict-shaped roles to
       title strings in place, leaves already-string roles untouched,
       and is a no-op on an empty/absent roles list.
    2. Unit -- ``classify_industry`` still requires ``roles: list[str]``
       (raises the documented TypeError on raw dicts) -- proving the fix
       is "normalize before calling", not a change to
       ``classify_industry`` itself, and pinning WHY skipping
       normalization crashes.
    3. Wiring -- source inspection confirming ``_run_async_generate``
       calls ``_normalize_dict_roles(gen_data)`` before it reads
       ``gen_data``'s roles for anything else, and that both the sync
       and async call sites route through the SAME helper (single
       source, no re-duplicated isinstance branch).
    4. Live end-to-end async run is covered separately in
       ``tests/test_generate_concurrency.py``
       (``test_async_generate_completes_with_dict_shaped_roles``), which
       exercises the real background worker thread.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402


# ---------------------------------------------------------------------------
# 1. _normalize_dict_roles -- unit
# ---------------------------------------------------------------------------


def test_normalize_dict_roles_flattens_dict_shaped_roles() -> None:
    data = {"target_roles": [{"title": "CDL A Driver", "count": 250}]}
    app._normalize_dict_roles(data)
    assert data["target_roles"] == ["CDL A Driver"]


def test_normalize_dict_roles_falls_back_to_role_key_then_repr() -> None:
    data = {
        "roles": [
            {"role": "Warehouse Associate", "count": 10},  # no "title" key
            {"count": 5},  # neither "title" nor "role"
        ]
    }
    app._normalize_dict_roles(data)
    assert data["roles"][0] == "Warehouse Associate"
    # No title/role -- falls back to str(dict), not silently dropped.
    assert data["roles"][1] == str({"count": 5})


def test_normalize_dict_roles_leaves_string_roles_untouched() -> None:
    data = {"target_roles": ["Warehouse Associate", "CDL A Driver"]}
    app._normalize_dict_roles(data)
    assert data["target_roles"] == ["Warehouse Associate", "CDL A Driver"]


def test_normalize_dict_roles_noop_on_missing_or_empty_roles() -> None:
    data: dict = {}
    app._normalize_dict_roles(data)
    assert data == {}

    data2 = {"target_roles": []}
    app._normalize_dict_roles(data2)
    assert data2["target_roles"] == []


def test_normalize_dict_roles_covers_both_roles_and_target_roles_keys() -> None:
    """The helper normalizes whichever key is populated -- the two /api/generate
    paths read roles via ``data.get("target_roles") or data.get("roles")``."""
    data = {"roles": [{"title": "Forklift Operator"}]}
    app._normalize_dict_roles(data)
    assert data["roles"] == ["Forklift Operator"]


# ---------------------------------------------------------------------------
# 2. classify_industry -- unit (documents the contract the helper protects)
# ---------------------------------------------------------------------------


def test_classify_industry_raises_on_raw_dict_roles() -> None:
    """Pin WHY skipping normalization crashes: classify_industry's own
    ``' '.join(roles or [])`` assumes list[str] and has no dict handling
    of its own -- callers MUST normalize first. This is not something the
    fix changes; it's the reason the fix normalizes before calling."""
    with pytest.raises(TypeError, match="expected str instance, dict found"):
        app.classify_industry(
            "", "Repro Co", [{"title": "CDL A Driver", "count": 250}]
        )


def test_classify_industry_succeeds_after_normalization() -> None:
    data = {"target_roles": [{"title": "CDL A Driver", "count": 250}]}
    app._normalize_dict_roles(data)
    profile = app.classify_industry("", "Repro Co", data["target_roles"])
    assert isinstance(profile, dict)
    assert profile.get("legacy_key")


# ---------------------------------------------------------------------------
# 3. Wiring -- both /api/generate paths route through the SAME helper
# ---------------------------------------------------------------------------


def _async_worker_source() -> str:
    """Slice app.py's source from ``_run_async_generate``'s def line
    through the start of its industry-classification block, so the
    normalization-call-comes-first assertion is scoped to the actual
    async worker instead of matching an unrelated part of the file."""
    src = (PROJECT_ROOT / "app.py").read_text()
    start = src.index("def _run_async_generate(jid, gen_data, rid):")
    end = src.index("industry_profile = classify_industry(", start)
    assert end > start, "could not locate async worker's classify_industry call"
    return src[start:end]


def test_async_worker_normalizes_roles_before_classifying_industry() -> None:
    block = _async_worker_source()
    norm_idx = block.index("_normalize_dict_roles(gen_data)")
    # No other read of gen_data's roles between the def line and the
    # normalization call -- it must run first, not after some other
    # consumer has already touched the raw (unnormalized) list.
    assert norm_idx < block.index("target_roles")


def test_sync_and_async_paths_call_the_same_normalize_helper() -> None:
    """Regression guard against re-introducing a duplicated isinstance
    branch: both call sites must invoke ``_normalize_dict_roles`` (one
    shared function), not each carry their own copy of the dict-flatten
    logic."""
    src = (PROJECT_ROOT / "app.py").read_text()
    call_count = src.count("_normalize_dict_roles(")
    # def + sync call site + async call site == 3 occurrences of the name.
    assert call_count == 3, (
        f"expected exactly 3 occurrences of _normalize_dict_roles( "
        f"(def + sync call + async call), found {call_count} -- if a new "
        "call site or a duplicated inline isinstance branch was added, "
        "update this count deliberately"
    )
    # Guard against the ORIGINAL bug's shape reappearing: an inline
    # "isinstance(_rlist[0], dict)" roles-flatten block outside this
    # helper's own definition.
    def_start = src.index("def _normalize_dict_roles(data: dict) -> None:")
    def_end = src.index("\n\n\n", def_start)
    body_outside_def = src[:def_start] + src[def_end:]
    assert "isinstance(_rlist[0], dict)" not in body_outside_def


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
