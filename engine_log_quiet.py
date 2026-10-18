"""Quiet the budget engine's per-call log lines for one bulk caller at a time.

``budget_engine`` logs a dozen INFO lines (and the odd WARNING) on every call.
That is useful for one request and noise for a batch: the health probe calls it
~900 times per sweep (4,400 lines in two minutes on 2026-10-16) and the hub
Prediction Engine grid calls it 560 times at every boot (6.7k-9.2k lines).

``with quiet_engine_logs():`` demotes those lines to DEBUG for the calling
thread only.  The flag is a ``threading.local``, so it is also per-greenlet
under gevent: concurrent request threads and other greenlets that run while
the batch is going keep their normal levels, and ERROR and above always pass.
A demoted line is still emitted if (and only if) DEBUG is enabled for the
logger, so ``LOG_LEVEL=DEBUG`` shows everything as before.
"""

from __future__ import annotations

import contextlib
import logging
import threading
from typing import Iterator

QUIET_LOGGERS = ("budget_engine",)

_ctx = threading.local()


class _DemoteEngineLogs(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not getattr(_ctx, "active", False):
            return True
        if record.levelno >= logging.ERROR or record.levelno <= logging.DEBUG:
            return True
        record.levelno, record.levelname = logging.DEBUG, "DEBUG"
        return logging.getLogger(record.name).isEnabledFor(logging.DEBUG)


def _install() -> None:
    demote = _DemoteEngineLogs()
    for name in QUIET_LOGGERS:
        engine_logger = logging.getLogger(name)
        # idempotent across importlib.reload (a reloaded class is a new class,
        # so match by name): never stack two filters
        for old in [
            f
            for f in engine_logger.filters
            if type(f).__name__ == _DemoteEngineLogs.__name__
        ]:
            engine_logger.removeFilter(old)
        engine_logger.addFilter(demote)


_install()


@contextlib.contextmanager
def quiet_engine_logs() -> Iterator[None]:
    """Demote the engine's INFO/WARNING lines to DEBUG on this thread only."""
    previous = getattr(_ctx, "active", False)
    _ctx.active = True
    try:
        yield
    finally:
        _ctx.active = previous
