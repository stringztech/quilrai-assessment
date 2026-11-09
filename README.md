# MCP and LLM Gateway Assessment

A Python implementation of the four Solution Architect tasks. Local upstreams use synthetic data and simulate side effects. A real OpenAI-compatible completion endpoint can replace either mock provider through configuration.

## Run locally

Use Python 3.12. From the repository root:

```sh
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

### 1. Customer MCP server

Run `python -m quilr_gateway.mcp_server` as the stdio command in an MCP client. Use the virtual environment's Python executable and the repository root as the working directory. The client initiates the MCP handshake. The test suite exercises that handshake and real subprocess I/O.

- `get_customer_record` accepts IDs such as `CUST-00001` and `CUST-12345`.
- `trigger_refund` accepts that ID, a finite positive JSON number, and a trimmed reason of at least ten characters. It returns a simulated result.
- Pydantic generates the advertised schemas and validates calls without coercing strings. Unknown fields are rejected. Invalid tool parameters return JSON-RPC `-32602`. A valid request for an unknown customer returns a tool error. Startup and SDK logs go to stderr.

### Start the HTTP components

Terminal one:

```sh
source .venv/bin/activate
uvicorn quilr_gateway.mock_upstream:app --host 127.0.0.1 --port 8001
```

Terminal two:

```sh
source .venv/bin/activate
export QUILR_MCP_ADMIN_TOKEN=local-admin
export QUILR_MCP_VIEWER_TOKEN=local-viewer
export QUILR_TENANT_TOKEN=local-tenant
uvicorn quilr_gateway.app:app --host 127.0.0.1 --port 8000
```

The short tokens are local demonstration values. For another environment, configure distinct high-entropy credentials and expose the gateway over TLS. Keep the downstream MCP endpoint on a private network so agents cannot bypass the gateway.

### 2. MCP tool authorization

```sh
curl -s http://127.0.0.1:8000/mcp \
  -H 'Authorization: Bearer local-viewer' -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'

curl -s http://127.0.0.1:8000/mcp \
  -H 'Authorization: Bearer local-viewer' -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"admin_reset_key"}}'
```

The list response is forwarded unchanged. The second request returns `-32001` and the original ID without reaching the upstream. Repeat with `local-admin` to exercise the permitted path. The gateway resolves roles from configured opaque tokens. It validates a single JSON-RPC envelope, rejects ambiguous duplicate keys and keeps caller credentials at the gateway. This is an HTTP POST/JSON proxy for the challenge's mock. Full MCP streamable HTTP sessions, GET/SSE, and OAuth discovery are outside this endpoint's scope. The stdio service in Task 1 runs independently.

### 3. Stream PII redaction

```sh
curl -sN http://127.0.0.1:8000/llm/stream \
  -H 'Authorization: Bearer local-tenant' -H 'Content-Type: application/json' \
  -d '{"prompt":"Run the synthetic stream demo","max_tokens":128}'
```

The gateway frames upstream SSE before decoding JSON, reconstructs text across model deltas, and emits safe text in `{"delta":"..."}` events. Expect three `[REDACTED]` markers and a final `[DONE]`. It commits text at safe boundaries as soon as possible and retains at most 320 characters between pieces. SSE event assembly has a separate 128 KiB bound. An interrupted stream discards the pending tail and emits a generic error event. Each downstream stream owns its own redactor state and closes its upstream response.

The patterns cover common ASCII emails, hyphenated US SSNs, and card-shaped 13–19-digit strings with optional spaces or hyphens. Card detection intentionally favors recall over precision, so some long numbers also get masked. This is text-pattern redaction for the requested examples. Other PII forms, encoded content, and language-dependent identifiers require additional detection policies.

### 4. Tenant rate limit and model fallback

```sh
curl -s http://127.0.0.1:8000/llm/complete \
  -H 'Authorization: Bearer local-tenant' -H 'Content-Type: application/json' \
  -d '{"prompt":"Demonstrate model routing","max_tokens":64}'
```

Restart the mock in terminal one with `QUILR_MOCK_PRIMARY_MODE=429` or `QUILR_MOCK_PRIMARY_MODE=timeout` before the same uvicorn command. Repeat the request and expect `"provider":"backup"`. The router tries the backup only on primary HTTP 429 or a three-second timeout. Each attempt has its own three-second deadline. Upstream failures produce a stable gateway error body. The streaming endpoint does not switch providers midstream.

The 50,000-token budget applies to a rolling 60-second window per authenticated tenant. A single SQLite writer transaction evicts expired rows, reads the tenant total, and reserves the new cost. Separate workers on the same host and database file share that budget. Admission charges the input estimate plus chat framing and the maximum requested output once per logical request. Failed requests retain their reservation until expiry, and retries are a separate provider billing concern. No prompt text or bearer tokens are stored in SQLite.

| Variable | Purpose |
|---|---|
| `QUILR_TENANT_KEYS` | JSON mapping of tenant names to distinct bearer tokens. Overrides the single demo tenant. |
| `QUILR_DB_PATH` | On-disk database path, default `state/limits.sqlite3`. |
| `QUILR_TOKEN_ENCODING` | Optional tiktoken encoding, such as `o200k_base`. For the offline mock, the default uses UTF-8 bytes as a conservative estimate for byte-based tokenizers. Set and pre-cache a compatible tokenizer for real model use. |
| `QUILR_PRIMARY_URL` / `QUILR_BACKUP_URL` | Full OpenAI-compatible chat completion URLs. |
| `QUILR_PRIMARY_MODEL` / `QUILR_BACKUP_MODEL` | Provider model names. |
| `QUILR_PRIMARY_KEY` / `QUILR_BACKUP_KEY` | Provider credentials supplied by the environment. |
| `QUILR_MCP_URL` | Fixed downstream MCP URL, defaulting to the loopback mock. |

For multiple hosts, replace the single-host SQLite budget with a shared atomic store. Calibrate chat framing to each provider and reconcile usage against provider metering if precise billing is required.

## Validation

The tests cover the stdio handshake and output separation, schema errors, proxy authorization and transparent forwarding, split PII and SSE frames, 429 and timeout failover, sanitized provider errors, and concurrency and expiry of the SQLite budget.
