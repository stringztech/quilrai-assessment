"""HTTP MCP authorization, streamed guardrails, and completion routing."""

import asyncio
import hmac
import json
import os
import sqlite3
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import anyio
import httpx
import tiktoken
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .limiter import TokenWindow
from .providers import Provider, UpstreamError, complete, text_deltas
from .redaction import StreamRedactor

MAX_BODY_BYTES = 128 * 1024
UNAUTHORIZED_TOOL_CALL = -32001


class CompletionRequest(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    prompt: Annotated[str, Field(min_length=1, max_length=40_000)]
    max_tokens: Annotated[int, Field(gt=0, le=8192)] = 512


@dataclass(frozen=True)
class Config:
    mcp_url: str
    mcp_roles: dict[str, str]  # role -> opaque token
    tenant_keys: dict[str, str]  # tenant -> opaque token
    primary: Provider
    backup: Provider
    db_path: Path
    token_encoding: str | None = None

    @classmethod
    def from_env(cls):
        return cls(
            mcp_url=os.getenv("QUILR_MCP_URL", "http://127.0.0.1:8001/mcp"),
            mcp_roles={
                "admin": os.getenv("QUILR_MCP_ADMIN_TOKEN", ""),
                "viewer": os.getenv("QUILR_MCP_VIEWER_TOKEN", ""),
            },
            tenant_keys=load_tenant_keys(),
            primary=Provider(
                os.getenv("QUILR_PRIMARY_URL", "http://127.0.0.1:8001/primary/v1/chat/completions"),
                os.getenv("QUILR_PRIMARY_MODEL", "demo-primary"),
                os.getenv("QUILR_PRIMARY_KEY", ""),
            ),
            backup=Provider(
                os.getenv("QUILR_BACKUP_URL", "http://127.0.0.1:8001/backup/v1/chat/completions"),
                os.getenv("QUILR_BACKUP_MODEL", "demo-backup"),
                os.getenv("QUILR_BACKUP_KEY", ""),
            ),
            db_path=Path(os.getenv("QUILR_DB_PATH", "state/limits.sqlite3")),
            token_encoding=os.getenv("QUILR_TOKEN_ENCODING") or None,
        )


def load_tenant_keys() -> dict[str, str]:
    configured = os.getenv("QUILR_TENANT_KEYS")
    if configured is None:
        return {"demo-tenant": os.getenv("QUILR_TENANT_TOKEN", "")}
    try:
        values = json.loads(configured)
    except ValueError:
        raise ValueError("QUILR_TENANT_KEYS must be a JSON object") from None
    if not isinstance(values, dict) or not values or any(
        not name or not isinstance(token, str) or not token
        or not token.isascii() or any(c.isspace() for c in token)
        for name, token in values.items()
    ) or len(set(values.values())) != len(values):
        raise ValueError("QUILR_TENANT_KEYS must map tenant names to distinct bearer tokens")
    return values


def authenticate(header: str, tokens: dict[str, str]) -> str | None:
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token or not token.isascii() or any(c.isspace() for c in token):
        return None
    matches = [
        identity for identity, known in tokens.items()
        if known and hmac.compare_digest(token.encode(), known.encode())
    ]
    return matches[0] if len(matches) == 1 else None


def rpc_error(code: int, message: str, request_id=None, *, status: int = 200):
    return JSONResponse(
        {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}},
        status_code=status,
    )


def gateway_error(code: str, message: str, status: int, *, headers=None):
    return JSONResponse({"error": {"code": code, "message": message}}, status, headers=headers)


def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    obj: dict[str, object] = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError("Duplicate JSON object key")
        obj[key] = value
    return obj


def reject_nonfinite(_: str):
    raise ValueError("Non-finite numbers are invalid JSON")


async def read_body(request: Request) -> bytes:
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > MAX_BODY_BYTES:
            raise OverflowError
        body.extend(chunk)
    return bytes(body)


def sse(payload: dict | str) -> str:
    encoded = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    return f"data: {encoded}\n\n"


def create_app(config: Config | None = None, *, transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    config = config or Config.from_env()
    limiter = TokenWindow(config.db_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await asyncio.to_thread(limiter.initialize)
        app.state.tokenizer = (
            await asyncio.to_thread(tiktoken.get_encoding, config.token_encoding)
            if config.token_encoding else None
        )
        async with httpx.AsyncClient(transport=transport, timeout=15, follow_redirects=False, trust_env=False) as client:
            app.state.upstream = client
            yield

    app = FastAPI(title="MCP and LLM Gateway", lifespan=lifespan)

    @app.post("/mcp")
    async def proxy_mcp(request: Request):
        role = authenticate(request.headers.get("authorization", ""), config.mcp_roles)
        if role is None:
            return rpc_error(-32000, "Authentication required", status=401)
        try:
            raw = await read_body(request)
            payload = json.loads(
                raw, object_pairs_hook=reject_duplicate_keys, parse_constant=reject_nonfinite
            )
        except OverflowError:
            return rpc_error(-32600, "Request too large", status=413)
        except (ValueError, UnicodeError, RecursionError):
            return rpc_error(-32700, "Parse error")

        # MCP sends one JSON-RPC message per HTTP POST. Never pass batches
        # downstream because that would bypass per-message authorization.
        if not isinstance(payload, dict) or payload.get("jsonrpc") != "2.0":
            return rpc_error(-32600, "Invalid request")
        request_id = payload.get("id")
        if request_id is not None and type(request_id) not in (str, int):
            return rpc_error(-32600, "Invalid request ID")
        method = payload.get("method")
        if not isinstance(method, str) or not method:
            return rpc_error(-32600, "Invalid request", request_id)
        if method == "tools/call":
            params = payload.get("params")
            if not isinstance(params, dict) or not isinstance(params.get("name"), str) or not params["name"]:
                return rpc_error(-32602, "Invalid tool name", request_id)
            if params["name"].startswith("admin_") and role != "admin":
                return rpc_error(UNAUTHORIZED_TOOL_CALL, "Unauthorized Tool Call", request_id)

        # Preserve protocol metadata and the downstream response. Do not send
        # caller credentials or hop-by-hop headers to the downstream service.
        forward_headers = {
            name: request.headers[name]
            for name in ("content-type", "accept", "mcp-protocol-version", "mcp-session-id")
            if name in request.headers
        }
        try:
            upstream = await app.state.upstream.post(config.mcp_url, content=raw, headers=forward_headers)
        except httpx.HTTPError:
            return rpc_error(-32002, "MCP upstream unavailable", request_id, status=502)
        response_headers = {
            name: upstream.headers[name]
            for name in ("content-type", "mcp-session-id", "mcp-protocol-version")
            if name in upstream.headers
        }
        return Response(upstream.content, upstream.status_code, headers=response_headers)

    async def admit(request: Request) -> CompletionRequest | JSONResponse:
        tenant = authenticate(request.headers.get("authorization", ""), config.tenant_keys)
        if tenant is None:
            return gateway_error("unauthorized", "Valid tenant credentials required.", 401)
        try:
            params = CompletionRequest.model_validate_json(await read_body(request))
        except OverflowError:
            return gateway_error("request_too_large", "Request body is too large.", 413)
        except ValidationError:
            return gateway_error("invalid_request", "Expected a prompt and a valid max_tokens value.", 400)

        # Charge prompt tokens plus framing and the reserved output budget once
        # per logical request. The loopback demo can run without tokenizer data.
        tokenizer = app.state.tokenizer
        if tokenizer is None:
            input_tokens = len(params.prompt.encode("utf-8"))
        else:
            input_tokens = len(await asyncio.to_thread(tokenizer.encode_ordinary, params.prompt))
        budget = input_tokens + 32 + params.max_tokens
        try:
            allowed = await limiter.reserve(tenant, budget)
        except sqlite3.Error:
            return gateway_error("limiter_unavailable", "Gateway temporarily unavailable.", 503)
        if not allowed:
            return gateway_error(
                "tenant_rate_limited", "Tenant token budget exceeded.", 429,
                headers={"Retry-After": str(limiter.seconds)},
            )
        return params

    @app.post("/llm/complete")
    async def route_completion(request: Request):
        params = await admit(request)
        if isinstance(params, JSONResponse):
            return params
        client = app.state.upstream
        try:
            content = await complete(client, config.primary, params.prompt, params.max_tokens)
            provider = "primary"
        except UpstreamError as error:
            if not error.retryable:
                return gateway_error("upstream_unavailable", "Primary model unavailable.", 502)
            try:
                content = await complete(client, config.backup, params.prompt, params.max_tokens)
                provider = "backup"
            except UpstreamError:
                return gateway_error("upstream_unavailable", "Model providers unavailable.", 502)

        redactor = StreamRedactor()
        redacted = "".join(redactor.feed(content)) + redactor.finish()
        return {"text": redacted, "provider": provider}

    @app.post("/llm/stream")
    async def guardrail_stream(request: Request):
        params = await admit(request)
        if isinstance(params, JSONResponse):
            return params
        client = app.state.upstream
        outbound = client.build_request(
            "POST", config.primary.url, headers=config.primary.headers(),
            json=config.primary.payload(params.prompt, params.max_tokens, stream=True),
        )
        try:
            upstream = await client.send(outbound, stream=True)
        except httpx.HTTPError:
            return gateway_error("upstream_unavailable", "Model stream unavailable.", 502)
        media_type = upstream.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if not upstream.is_success or media_type != "text/event-stream":
            await upstream.aclose()
            return gateway_error("upstream_unavailable", "Model stream unavailable.", 502)

        async def generate():
            redactor = StreamRedactor()
            try:
                async for delta in text_deltas(upstream):
                    for safe in redactor.feed(delta):
                        yield sse({"delta": safe})
                if tail := redactor.finish():
                    yield sse({"delta": tail})
                yield sse("[DONE]")
            except (httpx.HTTPError, UpstreamError):
                # Drop the unverified tail if the stream terminates early.
                yield sse({"error": {"code": "stream_failed", "message": "Model stream interrupted."}})
            finally:
                with anyio.CancelScope(shield=True):
                    await upstream.aclose()

        return StreamingResponse(
            generate(), media_type="text/event-stream",
            headers={"Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no"},
        )

    return app


app = create_app()
