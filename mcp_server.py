"""Nova MCP server -- expose the suite's data accessors as MCP tools (L2).

A minimal, dependency-free Model Context Protocol server over **stdio** using
JSON-RPC 2.0. It wraps EXISTING functions (it does not reimplement any data
logic) so an MCP client (Claude Desktop, an internal agent, etc.) can pull
first-party Joveo benchmarks and knowledge-base sections.

Design choices (per L2 brief):

* **No heavy framework.** The ``mcp`` PyPI package is not a dependency of this
  repo, so this module speaks the protocol directly with the Python stdlib. If
  ``mcp`` is later vendored in, this loop still works unchanged.
* **Internal-only, API-key gated.** Every ``tools/call`` is gated on the
  ``NOVA_MCP_API_KEY`` env var *when it is set*. The client passes the key in
  the tool arguments as ``api_key`` (or ``_api_key``). When the env var is unset
  the gate is open (local dev), matching the rest of the codebase's opt-in
  posture. ``initialize``/``tools/list`` are never gated so a client can
  discover the auth requirement.
* **Import-safe.** Importing this module has no side effects -- the heavy KB /
  Supabase accessors are imported lazily inside each tool, and the stdio loop
  only runs under ``if __name__ == "__main__"``.

Tools exposed:

* ``get_real_benchmark(title, location="")`` -> supabase_data.get_real_outcomes
  (the S89 keystone -- real Joveo cg_benchmarks campaign outcomes).
* ``get_channel_benchmark(channel, industry="overall")`` ->
  benchmark_registry.get_channel_benchmark (CPC/CPA, industry-adjusted, with a
  Supabase fallback via supabase_data.get_channel_benchmarks).
* ``query_kb(section)`` -> kb_loader.load_knowledge_base()[section].
* ``get_plan_inputs_schema()`` -> a static descriptor of the media-plan inputs.

Run it:

    NOVA_MCP_API_KEY=secret python3 mcp_server.py

It reads newline-delimited JSON-RPC requests on stdin and writes responses on
stdout (the MCP stdio transport). See MCP_SERVER_README.md for hosting.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("nova.mcp_server")

# MCP protocol revision this server implements. Clients echo their own; we
# answer with ours during initialize (clients tolerate a known-good string).
PROTOCOL_VERSION = "2024-11-05"
SERVER_NAME = "nova-media-plan"
SERVER_VERSION = "1.0.0"

# JSON-RPC 2.0 error codes (subset) + an app-specific auth code.
_ERR_PARSE = -32700
_ERR_INVALID_REQUEST = -32600
_ERR_METHOD_NOT_FOUND = -32601
_ERR_INVALID_PARAMS = -32602
_ERR_INTERNAL = -32603
_ERR_UNAUTHORIZED = -32001  # app-defined (server error range)

# Argument keys a client may use to pass the API key (stripped before dispatch).
_API_KEY_ARG_NAMES = ("api_key", "_api_key")


# ──────────────────────────────────────────────────────────────────────────
# Auth gate
# ──────────────────────────────────────────────────────────────────────────
def _expected_api_key() -> str:
    """Return the configured API key, or "" when auth is disabled.

    Read live (not cached at import) so tests and hosts can toggle the env var.
    """
    return (os.environ.get("NOVA_MCP_API_KEY") or "").strip()


def _check_auth(arguments: Dict[str, Any]) -> Optional[str]:
    """Validate the API key for a tool call.

    Returns ``None`` when authorized (gate open or key matches), otherwise a
    human-readable error string. Auth is enforced only when NOVA_MCP_API_KEY is
    set, preserving the existing local-dev behavior (open by default).
    """
    expected = _expected_api_key()
    if not expected:
        return None  # gate disabled -- internal/local use
    provided = ""
    for name in _API_KEY_ARG_NAMES:
        val = arguments.get(name)
        if isinstance(val, str) and val.strip():
            provided = val.strip()
            break
    if not provided:
        return "Missing API key. Pass 'api_key' in the tool arguments."
    # Constant-time comparison to avoid leaking key length/prefix via timing.
    import hmac

    if not hmac.compare_digest(provided, expected):
        return "Invalid API key."
    return None


def _strip_auth_args(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of ``arguments`` without the auth keys."""
    return {k: v for k, v in arguments.items() if k not in _API_KEY_ARG_NAMES}


# ──────────────────────────────────────────────────────────────────────────
# Tool implementations -- thin wrappers over EXISTING functions (lazy imports
# keep this module import-safe and side-effect-free).
# ──────────────────────────────────────────────────────────────────────────
def _tool_get_real_benchmark(title: str = "", location: str = "") -> Dict[str, Any]:
    """Real Joveo campaign outcomes for a job title (S89 keystone).

    Wraps supabase_data.get_real_outcomes. Returns ``{"matched": False}`` when
    the warehouse has no coverage (caller falls back to estimates).
    """
    import supabase_data

    return supabase_data.get_real_outcomes(str(title or ""), str(location or ""))


def _tool_get_channel_benchmark(
    channel: str = "", industry: str = "overall"
) -> Dict[str, Any]:
    """CPC/CPA benchmark for a channel, industry-adjusted.

    Wraps benchmark_registry.get_channel_benchmark (static + live Firecrawl
    overlay). Also attaches any matching Supabase ``channel_benchmarks`` rows
    under ``"supabase_rows"`` so callers get the richer first-party data when
    present, without changing the primary shape.
    """
    if not channel or not str(channel).strip():
        raise ValueError("channel is required")
    import benchmark_registry

    result = dict(benchmark_registry.get_channel_benchmark(str(channel), str(industry)))
    try:
        import supabase_data

        ind = "" if str(industry).lower() == "overall" else str(industry)
        rows = supabase_data.get_channel_benchmarks(str(channel), ind)
        if rows:
            result["supabase_rows"] = rows
    except Exception as exc:  # supabase is best-effort enrichment, never fatal
        logger.debug("supabase channel enrichment skipped: %s", exc)
    return result


def _tool_query_kb(section: str = "") -> Dict[str, Any]:
    """Return one section of the merged knowledge base.

    Wraps kb_loader.load_knowledge_base(). When ``section`` is empty or
    unknown, returns the list of available section keys instead of a payload so
    callers can discover what to ask for.
    """
    import kb_loader

    kb = kb_loader.load_knowledge_base()
    # Available sections = the canonical KB file keys present in the merged KB.
    available = sorted(k for k in getattr(kb_loader, "KB_FILES", {}) if k in kb)
    sec = str(section or "").strip()
    if not sec:
        return {"available_sections": available, "section": None, "data": None}
    if sec not in kb:
        return {
            "available_sections": available,
            "section": sec,
            "data": None,
            "error": f"Unknown section '{sec}'. See available_sections.",
        }
    return {"available_sections": available, "section": sec, "data": kb[sec]}


def _tool_get_plan_inputs_schema() -> Dict[str, Any]:
    """Static descriptor of the media-plan generator's inputs.

    Mirrors quick_plan.generate_quick_plan(role, location, budget, industry).
    Lets an MCP client know what to collect before asking the suite to build a
    plan. This is a descriptor only -- it triggers no computation.
    """
    return {
        "title": "Media Plan Inputs",
        "description": (
            "Inputs accepted by the Nova media-plan generator "
            "(quick_plan.generate_quick_plan)."
        ),
        "type": "object",
        "properties": {
            "role": {
                "type": "string",
                "description": "Job title to recruit for, e.g. 'Registered Nurse'.",
            },
            "location": {
                "type": "string",
                "description": "Location string, e.g. 'Dallas, TX', 'London', 'Remote'.",
            },
            "budget": {
                "type": ["number", "string"],
                "description": "Total campaign budget (USD unless currency given).",
            },
            "industry": {
                "type": "string",
                "description": "Optional industry key for benchmark context (e.g. 'healthcare').",
            },
        },
        "required": ["role", "location", "budget"],
    }


# Tool registry: name -> (handler, JSON-Schema input descriptor, description).
# The input schemas surface in tools/list so a client can build calls.
_TOOLS: Dict[str, Dict[str, Any]] = {
    "get_real_benchmark": {
        "handler": _tool_get_real_benchmark,
        "description": (
            "Real Joveo campaign outcomes (measured cost/apply) for a job title "
            "from the cg_benchmarks warehouse. Returns {'matched': false} when "
            "no first-party coverage exists."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Job title to match."},
                "location": {
                    "type": "string",
                    "description": "Optional location filter.",
                },
            },
            "required": ["title"],
        },
    },
    "get_channel_benchmark": {
        "handler": _tool_get_channel_benchmark,
        "description": (
            "CPC/CPA benchmark for a recruitment channel (e.g. 'indeed', "
            "'linkedin'), adjusted for industry. Includes Supabase rows when "
            "available."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "channel": {
                    "type": "string",
                    "description": "Channel/platform name, e.g. 'indeed'.",
                },
                "industry": {
                    "type": "string",
                    "description": "Industry key; defaults to 'overall'.",
                },
            },
            "required": ["channel"],
        },
    },
    "query_kb": {
        "handler": _tool_query_kb,
        "description": (
            "Read one section of the Nova knowledge base. Call with an empty "
            "section to list available sections."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "section": {
                    "type": "string",
                    "description": "KB section key, e.g. 'recruitment_benchmarks'.",
                }
            },
            "required": [],
        },
    },
    "get_plan_inputs_schema": {
        "handler": _tool_get_plan_inputs_schema,
        "description": (
            "Describe the inputs the media-plan generator accepts (role, "
            "location, budget, industry). Descriptor only -- no computation."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
}


def list_tools() -> List[Dict[str, Any]]:
    """Return the MCP tools/list payload (name, description, inputSchema)."""
    return [
        {
            "name": name,
            "description": spec["description"],
            "inputSchema": spec["inputSchema"],
        }
        for name, spec in _TOOLS.items()
    ]


def call_tool(name: str, arguments: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Dispatch a tool by name with the (auth-stripped) arguments.

    This is the testable core: it enforces the auth gate, validates the tool
    name, strips auth args, and invokes the handler. Returns an MCP
    ``tools/call`` result dict: ``{"content": [...], "isError": bool}`` with the
    JSON payload embedded as a text block (MCP's structured-result convention).

    Raises:
        PermissionError: when the API key gate rejects the call.
        KeyError: when ``name`` is not a registered tool.
    """
    arguments = arguments or {}
    auth_err = _check_auth(arguments)
    if auth_err is not None:
        raise PermissionError(auth_err)

    spec = _TOOLS.get(name)
    if spec is None:
        raise KeyError(name)

    handler: Callable[..., Any] = spec["handler"]
    kwargs = _strip_auth_args(arguments)
    payload = handler(**kwargs)
    return {
        "content": [{"type": "text", "text": json.dumps(payload, default=str)}],
        "structuredContent": payload if isinstance(payload, dict) else {"result": payload},
        "isError": False,
    }


# ──────────────────────────────────────────────────────────────────────────
# JSON-RPC 2.0 request handling (stdio transport)
# ──────────────────────────────────────────────────────────────────────────
def _rpc_result(req_id: Any, result: Any) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _rpc_error(req_id: Any, code: int, message: str) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def handle_request(request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Handle a single parsed JSON-RPC request object.

    Returns the response dict, or ``None`` for notifications (requests with no
    ``id``), which per JSON-RPC must not get a response.
    """
    req_id = request.get("id")
    is_notification = "id" not in request
    method = request.get("method")

    if request.get("jsonrpc") != "2.0" or not isinstance(method, str):
        if is_notification:
            return None
        return _rpc_error(req_id, _ERR_INVALID_REQUEST, "Invalid JSON-RPC request.")

    params = request.get("params") or {}

    if method == "initialize":
        result = {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
        }
        return None if is_notification else _rpc_result(req_id, result)

    if method in ("notifications/initialized", "initialized"):
        return None  # client ack -- no response

    if method == "ping":
        return None if is_notification else _rpc_result(req_id, {})

    if method == "tools/list":
        return (
            None
            if is_notification
            else _rpc_result(req_id, {"tools": list_tools()})
        )

    if method == "tools/call":
        name = params.get("name")
        arguments = params.get("arguments") or {}
        if not isinstance(name, str) or not name:
            return _rpc_error(req_id, _ERR_INVALID_PARAMS, "Missing tool name.")
        try:
            result = call_tool(name, arguments)
            return None if is_notification else _rpc_result(req_id, result)
        except PermissionError as exc:
            return _rpc_error(req_id, _ERR_UNAUTHORIZED, str(exc))
        except KeyError:
            return _rpc_error(
                req_id, _ERR_METHOD_NOT_FOUND, f"Unknown tool: {name}"
            )
        except (TypeError, ValueError) as exc:
            return _rpc_error(req_id, _ERR_INVALID_PARAMS, str(exc))
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("tool '%s' failed", name, exc_info=True)
            # Surface as an MCP tool error result, not a transport error, so the
            # client sees it as a failed tool rather than a broken server.
            err_result = {
                "content": [{"type": "text", "text": f"Tool error: {exc}"}],
                "isError": True,
            }
            return None if is_notification else _rpc_result(req_id, err_result)

    if is_notification:
        return None
    return _rpc_error(req_id, _ERR_METHOD_NOT_FOUND, f"Unknown method: {method}")


def serve_stdio(stdin: Any = None, stdout: Any = None) -> None:
    """Run the blocking stdio JSON-RPC loop.

    Reads newline-delimited JSON requests from ``stdin`` and writes
    newline-delimited JSON responses to ``stdout`` (the MCP stdio transport).
    Returns when stdin reaches EOF.
    """
    stdin = stdin if stdin is not None else sys.stdin
    stdout = stdout if stdout is not None else sys.stdout

    for line in stdin:
        line = line.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
        except json.JSONDecodeError:
            resp = _rpc_error(None, _ERR_PARSE, "Parse error: invalid JSON.")
            _write(stdout, resp)
            continue

        if isinstance(request, list):
            # JSON-RPC batch: handle each, emit only non-None responses.
            responses = [r for r in (handle_request(item) for item in request) if r]
            if responses:
                _write(stdout, responses)
            continue

        if not isinstance(request, dict):
            _write(stdout, _rpc_error(None, _ERR_INVALID_REQUEST, "Expected object."))
            continue

        response = handle_request(request)
        if response is not None:
            _write(stdout, response)


def _write(stdout: Any, obj: Any) -> None:
    """Write one JSON object/array as a single line and flush."""
    stdout.write(json.dumps(obj, default=str) + "\n")
    stdout.flush()


def main() -> None:
    """Entry point: configure logging to stderr and run the stdio loop.

    Logging goes to **stderr** so it never corrupts the JSON-RPC stream on
    stdout.
    """
    logging.basicConfig(
        level=os.environ.get("NOVA_MCP_LOG_LEVEL", "WARNING").upper(),
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if _expected_api_key():
        logger.info("Nova MCP server starting (API-key auth ENABLED).")
    else:
        logger.warning(
            "Nova MCP server starting (auth DISABLED -- set NOVA_MCP_API_KEY)."
        )
    try:
        serve_stdio()
    except KeyboardInterrupt:  # pragma: no cover
        pass


if __name__ == "__main__":
    main()
