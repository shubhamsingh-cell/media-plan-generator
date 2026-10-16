"""wsgi._run_deferred_startup() freezes the pre-loaded heap before fork
(2026-10-17, follow-up to the bae585c pre-load).

Under ``gunicorn --preload`` the master loads the KB, the gazetteer and the
SlotOps baselines, then forks; the workers inherit those pages
copy-on-write. But CPython's cyclic GC writes into every tracked container
header on each full collection, so a worker's first gen-2 pass privately
copies the shared pages and gives most of the saving back. gc.freeze()
moves the pre-loaded objects into the permanent generation, which later
collections never traverse. These tests pin that the freeze runs exactly
once, after both loaders, and that its failure never blocks boot.

Same loading technique as tests/test_wsgi_prewarm.py (wsgi.py exec'd under
stub app/auth modules with gevent's patch_all neutralised); gc.collect and
gc.freeze are stubbed so the pytest process itself is never frozen.
"""

from __future__ import annotations

import gc
import logging
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tests.test_wsgi_prewarm import _Counter, _load_wsgi  # noqa: E402


def _stub_gc(monkeypatch, order, freeze_exc=None):
    collect = _Counter("collect", order, result=0)
    freeze = _Counter("freeze", order, exc=freeze_exc)
    monkeypatch.setattr(gc, "collect", collect)
    monkeypatch.setattr(gc, "freeze", freeze)
    monkeypatch.setattr(gc, "get_freeze_count", lambda: 123456)
    return collect, freeze


def test_freeze_runs_once_after_both_loaders(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="wsgi")
    order = []
    geo = _Counter("geo", order)
    slot = _Counter("slot", order, result={"a": 1})
    collect, freeze = _stub_gc(monkeypatch, order)

    mod, stub_app = _load_wsgi(monkeypatch, geo=geo, slot=slot)

    assert order == ["geo", "slot", "collect", "freeze"]
    assert (collect.calls, freeze.calls) == (1, 1)
    assert stub_app._DEPLOY_WARMUP_COMPLETE is True
    infos = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    assert any(
        "gc.freeze(): 123456 objects moved to the permanent generation" in m
        for m in infos
    )

    # The _startup_done gate: a second call freezes nothing again.
    mod._run_deferred_startup()
    assert freeze.calls == 1


def test_freeze_failure_is_a_warning_and_boot_completes(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="wsgi")
    order = []
    geo = _Counter("geo", order)
    slot = _Counter("slot", order, result={"a": 1})
    _stub_gc(monkeypatch, order, freeze_exc=RuntimeError("freeze boom"))

    _, stub_app = _load_wsgi(monkeypatch, geo=geo, slot=slot)

    assert stub_app._DEPLOY_WARMUP_COMPLETE is True
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        "gc.freeze() after pre-load failed" in m and "freeze boom" in m
        for m in warnings
    )


def test_freeze_still_runs_when_a_loader_failed(monkeypatch):
    """A failed gazetteer load leaves the KB and baselines in the heap; they
    are still worth freezing."""
    order = []
    geo = _Counter("geo", order, exc=RuntimeError("gazetteer boom"))
    slot = _Counter("slot", order, result={"a": 1})
    _, freeze = _stub_gc(monkeypatch, order)

    _, stub_app = _load_wsgi(monkeypatch, geo=geo, slot=slot)

    assert freeze.calls == 1
    assert order == ["geo", "slot", "collect", "freeze"]
    assert stub_app._DEPLOY_WARMUP_COMPLETE is True
