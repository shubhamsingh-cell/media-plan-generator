"""Tests for mcp_server -- tool dispatch, auth gate, and JSON-RPC handling.

All wrapped data accessors (supabase_data, benchmark_registry, kb_loader) are
mocked so the suite runs offline with no Supabase/KB access. Importing
mcp_server must have no side effects.

Runs under pytest, or standalone: ``python3 tests/test_mcp_server.py``.
"""

import io
import json
import os
import sys
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import mcp_server  # noqa: E402


# ── helpers ────────────────────────────────────────────────────────────────
def _no_key(monkeypatch=None):
    """Ensure the auth gate is disabled (env var unset)."""
    os.environ.pop("NOVA_MCP_API_KEY", None)


def _payload_from_result(result):
    """Extract the JSON payload embedded in an MCP tools/call result."""
    return json.loads(result["content"][0]["text"])


# ── import safety ────────────────────────────────────────────────────────────
def test_import_is_side_effect_free():
    # Re-importing must not raise and the module exposes its registry.
    assert isinstance(mcp_server._TOOLS, dict)
    assert set(mcp_server._TOOLS) == {
        "get_real_benchmark",
        "get_channel_benchmark",
        "query_kb",
        "get_plan_inputs_schema",
    }


# ── tools/list ───────────────────────────────────────────────────────────────
def test_list_tools_shape():
    tools = mcp_server.list_tools()
    names = {t["name"] for t in tools}
    assert names == set(mcp_server._TOOLS)
    for t in tools:
        assert isinstance(t["description"], str) and t["description"]
        assert t["inputSchema"]["type"] == "object"


# ── auth gate ────────────────────────────────────────────────────────────────
def test_auth_gate_open_when_env_unset():
    _no_key()
    # No key required -> _check_auth returns None even with empty args.
    assert mcp_server._check_auth({}) is None


def test_auth_gate_rejects_missing_key():
    with mock.patch.dict(os.environ, {"NOVA_MCP_API_KEY": "s3cret"}):
        err = mcp_server._check_auth({})
        assert err and "Missing API key" in err


def test_auth_gate_rejects_wrong_key():
    with mock.patch.dict(os.environ, {"NOVA_MCP_API_KEY": "s3cret"}):
        err = mcp_server._check_auth({"api_key": "nope"})
        assert err and "Invalid API key" in err


def test_auth_gate_accepts_correct_key():
    with mock.patch.dict(os.environ, {"NOVA_MCP_API_KEY": "s3cret"}):
        assert mcp_server._check_auth({"api_key": "s3cret"}) is None
        assert mcp_server._check_auth({"_api_key": "s3cret"}) is None


def test_call_tool_raises_permission_error_without_key():
    with mock.patch.dict(os.environ, {"NOVA_MCP_API_KEY": "s3cret"}):
        try:
            mcp_server.call_tool("get_plan_inputs_schema", {})
        except PermissionError as exc:
            assert "Missing API key" in str(exc)
        else:
            raise AssertionError("expected PermissionError")


def test_call_tool_strips_auth_arg_before_dispatch():
    # With a valid key the auth arg must NOT be forwarded to the handler.
    m = mock.MagicMock(return_value={"matched": False})
    original = mcp_server._TOOLS["get_real_benchmark"]["handler"]
    mcp_server._TOOLS["get_real_benchmark"]["handler"] = m
    try:
        with mock.patch.dict(os.environ, {"NOVA_MCP_API_KEY": "s3cret"}):
            res = mcp_server.call_tool(
                "get_real_benchmark",
                {"api_key": "s3cret", "title": "Nurse", "location": "TX"},
            )
    finally:
        mcp_server._TOOLS["get_real_benchmark"]["handler"] = original
    # auth arg stripped -> only the real tool kwargs reach the handler
    m.assert_called_once_with(title="Nurse", location="TX")
    assert res["isError"] is False


# ── tool dispatch (handlers mocked) ─────────────────────────────────────────
def test_dispatch_get_real_benchmark():
    _no_key()
    fake = {"matched": True, "cost_per_apply": 4.2, "source": "warehouse"}
    with mock.patch.dict(sys.modules, {"supabase_data": mock.MagicMock()}):
        sys.modules["supabase_data"].get_real_outcomes.return_value = fake
        res = mcp_server.call_tool(
            "get_real_benchmark", {"title": "RN", "location": "Dallas"}
        )
        sys.modules["supabase_data"].get_real_outcomes.assert_called_once_with(
            "RN", "Dallas"
        )
    assert _payload_from_result(res) == fake
    assert res["structuredContent"] == fake


def test_dispatch_get_channel_benchmark_requires_channel():
    _no_key()
    try:
        mcp_server.call_tool("get_channel_benchmark", {"channel": ""})
    except ValueError as exc:
        assert "channel is required" in str(exc)
    else:
        raise AssertionError("expected ValueError for empty channel")


def test_dispatch_get_channel_benchmark():
    _no_key()
    base = {"cpc": 1.5, "cpa": 30.0, "industry": "healthcare"}
    breg = mock.MagicMock()
    breg.get_channel_benchmark.return_value = base
    sdata = mock.MagicMock()
    sdata.get_channel_benchmarks.return_value = [{"channel": "indeed", "cpc": 1.6}]
    with mock.patch.dict(
        sys.modules, {"benchmark_registry": breg, "supabase_data": sdata}
    ):
        res = mcp_server.call_tool(
            "get_channel_benchmark", {"channel": "indeed", "industry": "healthcare"}
        )
    payload = _payload_from_result(res)
    assert payload["cpc"] == 1.5
    assert payload["supabase_rows"] == [{"channel": "indeed", "cpc": 1.6}]
    breg.get_channel_benchmark.assert_called_once_with("indeed", "healthcare")


def test_dispatch_query_kb_lists_sections_when_empty():
    _no_key()
    fake_kb = mock.MagicMock()
    fake_kb.load_knowledge_base.return_value = {"core": {"x": 1}, "regional_hiring": {}}
    fake_kb.KB_FILES = {"core": "core.json", "regional_hiring": "rh.json"}
    with mock.patch.dict(sys.modules, {"kb_loader": fake_kb}):
        res = mcp_server.call_tool("query_kb", {"section": ""})
    payload = _payload_from_result(res)
    assert payload["data"] is None
    assert set(payload["available_sections"]) == {"core", "regional_hiring"}


def test_dispatch_query_kb_returns_section():
    _no_key()
    fake_kb = mock.MagicMock()
    fake_kb.load_knowledge_base.return_value = {"core": {"x": 1}}
    fake_kb.KB_FILES = {"core": "core.json"}
    with mock.patch.dict(sys.modules, {"kb_loader": fake_kb}):
        res = mcp_server.call_tool("query_kb", {"section": "core"})
    payload = _payload_from_result(res)
    assert payload["section"] == "core"
    assert payload["data"] == {"x": 1}


def test_dispatch_query_kb_unknown_section():
    _no_key()
    fake_kb = mock.MagicMock()
    fake_kb.load_knowledge_base.return_value = {"core": {"x": 1}}
    fake_kb.KB_FILES = {"core": "core.json"}
    with mock.patch.dict(sys.modules, {"kb_loader": fake_kb}):
        res = mcp_server.call_tool("query_kb", {"section": "nope"})
    payload = _payload_from_result(res)
    assert payload["data"] is None
    assert "Unknown section" in payload["error"]


def test_dispatch_get_plan_inputs_schema():
    _no_key()
    res = mcp_server.call_tool("get_plan_inputs_schema", {})
    payload = _payload_from_result(res)
    assert payload["required"] == ["role", "location", "budget"]
    assert set(payload["properties"]) == {"role", "location", "budget", "industry"}


def test_call_tool_unknown_name_raises_keyerror():
    _no_key()
    try:
        mcp_server.call_tool("does_not_exist", {})
    except KeyError:
        pass
    else:
        raise AssertionError("expected KeyError for unknown tool")


# ── JSON-RPC layer ───────────────────────────────────────────────────────────
def test_handle_initialize():
    resp = mcp_server.handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
    )
    assert resp["id"] == 1
    assert resp["result"]["serverInfo"]["name"] == mcp_server.SERVER_NAME
    assert resp["result"]["protocolVersion"] == mcp_server.PROTOCOL_VERSION


def test_handle_tools_list():
    resp = mcp_server.handle_request(
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
    )
    assert {t["name"] for t in resp["result"]["tools"]} == set(mcp_server._TOOLS)


def test_handle_notification_returns_none():
    # No "id" -> notification -> no response.
    assert (
        mcp_server.handle_request(
            {"jsonrpc": "2.0", "method": "notifications/initialized"}
        )
        is None
    )


def test_handle_unknown_method():
    resp = mcp_server.handle_request(
        {"jsonrpc": "2.0", "id": 3, "method": "bogus/method"}
    )
    assert resp["error"]["code"] == mcp_server._ERR_METHOD_NOT_FOUND


def test_handle_invalid_request():
    resp = mcp_server.handle_request({"id": 4, "method": 123})
    assert resp["error"]["code"] == mcp_server._ERR_INVALID_REQUEST


def test_handle_tools_call_unauthorized_maps_to_error():
    with mock.patch.dict(os.environ, {"NOVA_MCP_API_KEY": "s3cret"}):
        resp = mcp_server.handle_request(
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {"name": "get_plan_inputs_schema", "arguments": {}},
            }
        )
    assert resp["error"]["code"] == mcp_server._ERR_UNAUTHORIZED


def test_handle_tools_call_missing_name():
    resp = mcp_server.handle_request(
        {"jsonrpc": "2.0", "id": 6, "method": "tools/call", "params": {}}
    )
    assert resp["error"]["code"] == mcp_server._ERR_INVALID_PARAMS


def test_handle_tools_call_success():
    _no_key()
    resp = mcp_server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {"name": "get_plan_inputs_schema", "arguments": {}},
        }
    )
    assert resp["result"]["isError"] is False
    payload = json.loads(resp["result"]["content"][0]["text"])
    assert payload["required"] == ["role", "location", "budget"]


def test_handle_tool_internal_error_is_tool_error_result():
    _no_key()
    boom = mock.MagicMock(side_effect=RuntimeError("kaboom"))
    mcp_server._TOOLS["get_plan_inputs_schema"]["handler"] = boom
    try:
        resp = mcp_server.handle_request(
            {
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tools/call",
                "params": {"name": "get_plan_inputs_schema", "arguments": {}},
            }
        )
    finally:
        mcp_server._TOOLS["get_plan_inputs_schema"][
            "handler"
        ] = mcp_server._tool_get_plan_inputs_schema
    # Internal handler errors surface as a tool-error RESULT, not a transport error.
    assert "error" not in resp
    assert resp["result"]["isError"] is True


# ── stdio loop (end-to-end over fake streams) ───────────────────────────────
def test_serve_stdio_initialize_and_call():
    _no_key()
    requests = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},  # notification
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "get_plan_inputs_schema", "arguments": {}},
        },
    ]
    stdin = io.StringIO("\n".join(json.dumps(r) for r in requests) + "\n")
    stdout = io.StringIO()
    mcp_server.serve_stdio(stdin=stdin, stdout=stdout)
    lines = [ln for ln in stdout.getvalue().splitlines() if ln.strip()]
    # 2 responses (notification produced none).
    assert len(lines) == 2
    init_resp = json.loads(lines[0])
    assert init_resp["result"]["serverInfo"]["name"] == mcp_server.SERVER_NAME
    call_resp = json.loads(lines[1])
    assert call_resp["id"] == 2
    assert call_resp["result"]["isError"] is False


def test_serve_stdio_parse_error():
    _no_key()
    stdin = io.StringIO("not json\n")
    stdout = io.StringIO()
    mcp_server.serve_stdio(stdin=stdin, stdout=stdout)
    resp = json.loads(stdout.getvalue().strip())
    assert resp["error"]["code"] == mcp_server._ERR_PARSE


if __name__ == "__main__":
    _failures = 0
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            try:
                _fn()
                print(f"PASS {_name}")
            except AssertionError as exc:
                _failures += 1
                print(f"FAIL {_name}: {exc}")
            except Exception as exc:  # noqa: BLE001
                _failures += 1
                print(f"ERROR {_name}: {exc}")
    sys.exit(1 if _failures else 0)
