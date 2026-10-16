"""wsgi._run_deferred_startup() pre-loads the two big reference datasets.

Prod runs ``gunicorn --preload wsgi:app``. gunicorn's Arbiter.setup() calls
``self.app.wsgi()`` -- which imports ``wsgi`` -- in the MASTER, before any
worker is forked, and wsgi.py ends with a module-level
``_run_deferred_startup()``. So everything that function loads lands in the
master heap and every forked worker inherits it copy-on-write.

Before this change the plan_location gazetteer (~65 MB: rows + indexes) and
slotops_baseline_data.json (~24 MB) were NOT pre-loaded, so each worker parsed
a private copy on its first /api/locations/resolve and its first generation
(fresh workers ~152 MB RSS, warmed ones ~211 MB each).

These tests pin that both loaders run exactly once from the preload path, in
the order KB -> gazetteer -> baselines, and that a failure in one never blocks
the other or the rest of boot.

wsgi.py is loaded from source under stub ``app`` / ``auth`` modules with
gevent's ``monkey.patch_all`` neutralised: importing the real thing in the
pytest process would monkey-patch stdlib sockets/threading for every later
test and flip app's auth/warm-up flags. The loaders themselves are the real
modules' attributes, monkeypatched per test, so nothing reads the datasets
from disk.
"""

import importlib.util
import logging
import sys
import types
from http.server import BaseHTTPRequestHandler
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

gevent_monkey = pytest.importorskip("gevent.monkey")


def _load_wsgi(monkeypatch, *, geo, slot, kb=None):
    """Exec wsgi.py as gunicorn --preload would (import => deferred startup).

    Returns (wsgi_module, stub_app). Loader patches are applied BEFORE the
    exec so the module-level ``_run_deferred_startup()`` call -- the actual
    preload path -- hits them.
    """
    monkeypatch.setattr(gevent_monkey, "patch_all", lambda *a, **k: None)

    stub_app = types.ModuleType("app")

    class MediaPlanHandler(BaseHTTPRequestHandler):
        pass

    stub_app.MediaPlanHandler = MediaPlanHandler
    stub_app.load_knowledge_base = kb or (lambda: {"k": 1})
    stub_app._vector_search_available = False
    stub_app._vector_index_kb = None
    stub_app._auth_module_loaded = None
    stub_app._DEPLOY_WARMUP_COMPLETE = False
    monkeypatch.setitem(sys.modules, "app", stub_app)

    stub_auth = types.ModuleType("auth")
    stub_auth.init = lambda: None
    stub_auth.is_auth_enabled = lambda: False
    monkeypatch.setitem(sys.modules, "auth", stub_auth)

    import plan_location
    import slotops_engine

    monkeypatch.setattr(plan_location, "_ensure_loaded", geo)
    monkeypatch.setattr(slotops_engine, "load_baselines", slot)

    spec = importlib.util.spec_from_file_location("wsgi", PROJECT_ROOT / "wsgi.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod, stub_app


class _Counter:
    """Callable that records calls into a shared ordered list."""

    def __init__(self, name, order, result=None, exc=None):
        self.name = name
        self.order = order
        self.result = result
        self.exc = exc
        self.calls = 0

    def __call__(self, *args, **kwargs):
        self.calls += 1
        self.order.append(self.name)
        if self.exc is not None:
            raise self.exc
        return self.result


def test_preload_import_runs_both_loaders_once_after_kb(monkeypatch):
    """Importing wsgi (== gunicorn --preload in the master) loads the KB, then
    the gazetteer, then the SlotOps baselines -- each exactly once."""
    order = []
    geo = _Counter("geo", order)
    slot = _Counter("slot", order, result={"country_baselines": {}, "dow": {}})
    kb = _Counter("kb", order, result={"k": 1})

    mod, stub_app = _load_wsgi(monkeypatch, geo=geo, slot=slot, kb=kb)

    assert (kb.calls, geo.calls, slot.calls) == (1, 1, 1)
    assert order == ["kb", "geo", "slot"]
    assert stub_app._DEPLOY_WARMUP_COMPLETE is True

    # The _startup_done gate keeps a second call from reloading anything.
    mod._run_deferred_startup()
    assert (kb.calls, geo.calls, slot.calls) == (1, 1, 1)


def test_direct_call_runs_each_loader_once(monkeypatch):
    """_run_deferred_startup() invoked directly (gate reset) calls each loader
    exactly once more -- not zero, not twice."""
    order = []
    geo = _Counter("geo", order)
    slot = _Counter("slot", order, result={"a": 1})
    mod, _ = _load_wsgi(monkeypatch, geo=geo, slot=slot)

    geo.calls = slot.calls = 0
    mod._startup_done = False
    mod._run_deferred_startup()

    assert (geo.calls, slot.calls) == (1, 1)


def test_geo_failure_does_not_block_slotops_or_boot(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="wsgi")
    order = []
    geo = _Counter("geo", order, exc=RuntimeError("gazetteer boom"))
    slot = _Counter("slot", order, result={"a": 1})

    _, stub_app = _load_wsgi(monkeypatch, geo=geo, slot=slot)

    assert (geo.calls, slot.calls) == (1, 1)
    assert stub_app._DEPLOY_WARMUP_COMPLETE is True  # boot ran to the end
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("plan_location pre-warm failed" in m and "gazetteer boom" in m for m in warnings)


def test_slotops_failure_does_not_block_geo_or_boot(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="wsgi")
    order = []
    geo = _Counter("geo", order)
    slot = _Counter("slot", order, exc=ValueError("baselines boom"))

    _, stub_app = _load_wsgi(monkeypatch, geo=geo, slot=slot)

    assert (geo.calls, slot.calls) == (1, 1)
    assert stub_app._DEPLOY_WARMUP_COMPLETE is True
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("SlotOps baselines pre-warm failed" in m and "baselines boom" in m for m in warnings)


def test_empty_baselines_warn_but_never_raise(monkeypatch, caplog):
    """load_baselines() swallows file/JSON errors and returns {}; that must
    surface as a warning (it retries lazily per worker), not an exception."""
    caplog.set_level(logging.INFO, logger="wsgi")
    order = []
    geo = _Counter("geo", order)
    slot = _Counter("slot", order, result={})

    _, stub_app = _load_wsgi(monkeypatch, geo=geo, slot=slot)

    assert (geo.calls, slot.calls) == (1, 1)
    assert stub_app._DEPLOY_WARMUP_COMPLETE is True
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("SlotOps baselines pre-warm returned no data" in m for m in warnings)


def test_elapsed_time_is_logged_for_each_loader(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="wsgi")
    order = []
    geo = _Counter("geo", order)
    slot = _Counter("slot", order, result={"a": 1, "b": 2})

    _load_wsgi(monkeypatch, geo=geo, slot=slot)

    infos = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    assert any("plan_location gazetteer pre-warmed in" in m and m.endswith("s") for m in infos)
    assert any("SlotOps baselines pre-warmed: 2 sections in" in m for m in infos)
