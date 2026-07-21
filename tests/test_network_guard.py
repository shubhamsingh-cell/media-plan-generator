#!/usr/bin/env python3
"""Regression tests for the two layers that ended the 2026-07-17 ship-gate hang.

Layer 1 (mechanism): vector_search/tavily_search read their API keys from the
environment fresh on every call -- the old write-once module-global cache let
a ``mock.patch.dict(os.environ, ...)`` in one test permanently poison every
later test in the pytest process (commit 4cac3116's root cause).

Layer 2 (structure): the conftest network guard blocks any non-loopback TCP
connect during pytest, so a future unmocked external call fails in
milliseconds with an unmistakable error instead of hanging the gate suite.
"""

from __future__ import annotations

import os
import socket
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import tavily_search  # noqa: E402
import vector_search  # noqa: E402

# Non-routable RFC1918 address: without the guard, connecting here would
# block for the full OS SYN-retry window (~75s) -- the exact hang class the
# guard exists to kill. settimeout(5) is a belt so a guard regression makes
# this test FAIL in seconds rather than hang the suite that guards the gate.
_NON_ROUTABLE = ("10.255.255.1", 443)


# ── Layer 2: conftest network guard ──────────────────────────────────────────


class TestNetworkGuard:
    def test_external_connect_blocked_fast_with_distinctive_error(self) -> None:
        start = time.monotonic()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(5)
            with pytest.raises(OSError, match="network guard"):
                sock.connect(_NON_ROUTABLE)
        assert time.monotonic() - start < 2.0, "guard must fail fast, not hang"

    def test_external_connect_ex_refused_without_raising(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(5)
            assert sock.connect_ex(_NON_ROUTABLE) != 0  # connect_ex never raises

    def test_loopback_connect_still_allowed(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind(("127.0.0.1", 0))
            server.listen(1)
            port = server.getsockname()[1]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.settimeout(5)
                client.connect(("127.0.0.1", port))  # must not raise

    def test_urllib_external_call_degrades_as_urlerror(self) -> None:
        # Production code catches URLError/OSError for graceful fallback; the
        # guard's error must surface through urllib the same way a dead
        # upstream would, only instantly. Numeric-IP URL: no DNS dependency,
        # so this passes identically on hosts without a resolver.
        start = time.monotonic()
        with pytest.raises(urllib.error.URLError, match="network guard"):
            urllib.request.urlopen("https://10.255.255.1/", timeout=5)
        assert time.monotonic() - start < 2.0

    @pytest.mark.live
    def test_live_marker_is_exempted_from_guard(self) -> None:
        # The autouse fixture must have set the escape hatch for this test
        # (no real network is touched here -- this pins the plumbing that
        # lets keyed environments run their real `live` probes).
        assert os.environ.get("NOVA_TESTS_ALLOW_NET") == "1"


# ── Layer 1: no lazy env-key caching ─────────────────────────────────────────


class TestApiKeyEnvIsNeverCached:
    @pytest.mark.parametrize(
        "module,env_var,seam",
        [
            (vector_search, "VOYAGE_API_KEY", "_VOYAGE_API_KEY"),
            (tavily_search, "TAVILY_API_KEY", "_TAVILY_API_KEY"),
        ],
    )
    def test_env_patch_cannot_poison_later_calls(self, module, env_var, seam) -> None:
        baseline = os.environ.get(env_var) or None
        with mock.patch.dict("os.environ", {env_var: "poison"}, clear=False):
            assert module._get_api_key() == "poison"
        # After the patch exits: reads track the restored environment, and
        # the override seam was never written -- the 4cac3116 bug is dead.
        assert module._get_api_key() == baseline
        assert getattr(module, seam) is None

    @pytest.mark.parametrize("module", [vector_search, tavily_search])
    def test_attr_seam_still_overrides(self, module) -> None:
        with mock.patch.object(module, "_get_api_key", wraps=module._get_api_key):
            pass  # sanity: attribute exists and is patchable
        seam = "_VOYAGE_API_KEY" if module is vector_search else "_TAVILY_API_KEY"
        with mock.patch.object(module, seam, "k"):
            assert module._get_api_key() == "k"
        with mock.patch.object(module, seam, ""):
            assert module._get_api_key() is None  # explicit no-key override
