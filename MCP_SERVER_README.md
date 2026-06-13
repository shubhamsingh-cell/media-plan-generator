# Nova MCP Server

`mcp_server.py` exposes the Nova / Media-Plan-Generator data accessors as a
[Model Context Protocol](https://modelcontextprotocol.io) (MCP) server over
**stdio**, speaking JSON-RPC 2.0. It is **internal-only** and **API-key gated**
— the chosen default for L2.

It is a thin wrapper: every tool calls an **existing** function in the codebase
(no data logic is reimplemented). The whole server is the Python standard
library — there is **no framework dependency** (the `mcp` PyPI package is not
required and is not installed in this repo).

---

## Tools

| Tool | Wraps | Arguments | Returns |
|------|-------|-----------|---------|
| `get_real_benchmark` | `supabase_data.get_real_outcomes` | `title` (required), `location` | Real Joveo cg_benchmarks campaign outcomes (measured cost/apply). `{"matched": false}` when the warehouse has no coverage. |
| `get_channel_benchmark` | `benchmark_registry.get_channel_benchmark` (+ `supabase_data.get_channel_benchmarks` enrichment) | `channel` (required), `industry` (default `"overall"`) | CPC/CPA benchmark, industry-adjusted, with live-Firecrawl overlay. Adds `supabase_rows` when first-party rows exist. |
| `query_kb` | `kb_loader.load_knowledge_base` | `section` | One section of the merged knowledge base. Call with an empty `section` to list `available_sections`. |
| `get_plan_inputs_schema` | (static descriptor) | none | JSON-Schema-style descriptor of media-plan inputs (`role`, `location`, `budget`, `industry`), mirroring `quick_plan.generate_quick_plan`. |

Each tool's `inputSchema` is published via `tools/list` so a client can build
calls without reading this file.

### Result shape

`tools/call` returns the MCP convention: a `content` array with a single `text`
block containing the JSON payload, plus a `structuredContent` mirror and an
`isError` flag.

```json
{
  "content": [{"type": "text", "text": "{\"matched\": false}"}],
  "structuredContent": {"matched": false},
  "isError": false
}
```

Internal handler errors are returned as a tool-error **result** (`isError:
true`), not a JSON-RPC transport error, so a client sees a failed tool rather
than a broken connection. Bad requests (unknown method, unknown tool, missing
params, bad auth) are returned as JSON-RPC `error` objects.

---

## Authentication

Auth is gated on the `NOVA_MCP_API_KEY` environment variable, matching the rest
of the codebase's opt-in posture:

- **`NOVA_MCP_API_KEY` set** → every `tools/call` must include the key in the
  tool arguments as `api_key` (or `_api_key`). The key is compared in constant
  time and is **stripped** before the arguments reach the handler. A bad/missing
  key returns JSON-RPC error code `-32001`.
- **`NOVA_MCP_API_KEY` unset** → the gate is open (local development). No
  behavior changes for existing callers.

`initialize` and `tools/list` are **never** gated, so a client can always
discover the server and learn that auth is required. Pass the key per call:

```json
{"jsonrpc":"2.0","id":3,"method":"tools/call",
 "params":{"name":"get_real_benchmark",
           "arguments":{"api_key":"YOUR_KEY","title":"Registered Nurse","location":"Dallas, TX"}}}
```

> Generate a key with e.g. `python3 -c "import secrets; print(secrets.token_urlsafe(32))"`
> and store it in your secret manager — never commit it.

---

## Running it

The server reads newline-delimited JSON-RPC requests on **stdin** and writes
responses on **stdout**. All logging goes to **stderr** so it never corrupts the
protocol stream.

```bash
# Local dev (auth disabled):
python3 mcp_server.py

# Internal/production (auth enabled):
NOVA_MCP_API_KEY=your-secret python3 mcp_server.py

# Optional: raise log verbosity (defaults to WARNING).
NOVA_MCP_LOG_LEVEL=INFO NOVA_MCP_API_KEY=your-secret python3 mcp_server.py
```

### Smoke test from the shell

```bash
printf '%s\n%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
  | python3 mcp_server.py
```

### Hosting it for an MCP client (e.g. Claude Desktop)

Add a stdio server entry pointing at this file. Example
`claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "nova-media-plan": {
      "command": "python3",
      "args": ["/absolute/path/to/media-plan-generator/mcp_server.py"],
      "env": {
        "NOVA_MCP_API_KEY": "your-secret"
      }
    }
  }
}
```

Because auth is per-call, the client must include `api_key` in each tool's
arguments when `NOVA_MCP_API_KEY` is set. (If your client cannot inject a static
argument, run the server with the env var unset on a trusted, network-isolated
host — internal-only by design.)

The server is import-safe (no side effects on `import mcp_server`) and the heavy
KB/Supabase accessors are imported lazily inside each tool, so startup is fast
and the module can be imported by tests without booting the warehouse.

---

## Protocol

- JSON-RPC 2.0, MCP protocol version `2024-11-05`.
- Methods handled: `initialize`, `notifications/initialized` (ack, no response),
  `ping`, `tools/list`, `tools/call`. Notifications (requests without `id`) never
  receive a response. JSON-RPC batches (arrays) are supported.

---

## Tests

```bash
python3 -m py_compile mcp_server.py
python3 -m pytest tests/test_mcp_server.py -q
# or standalone, no pytest:
python3 tests/test_mcp_server.py
```

The tests mock Supabase / KB / benchmark accessors, so they run fully offline.
They cover tool dispatch, the auth gate (open / missing / wrong / correct key,
and arg-stripping), the JSON-RPC layer, and an end-to-end stdio round-trip.
