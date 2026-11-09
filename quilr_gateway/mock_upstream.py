"""Loopback-only mock services for demonstrations and development."""

import asyncio
import json
import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

app = FastAPI(title="Synthetic upstreams")


@app.post("/mcp")
async def mcp_mock(request: Request):
    payload = await request.json()
    method = payload.get("method")
    if "id" not in payload:
        return Response(status_code=202)
    response = {"jsonrpc": "2.0", "id": payload["id"]}
    if method == "initialize":
        result = {
            "protocolVersion": payload.get("params", {}).get("protocolVersion", "2025-11-25"),
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "mock-mcp", "version": "1.0"},
        }
    elif method == "tools/list":
        result = {"tools": [
            {"name": name, "inputSchema": {"type": "object", "additionalProperties": False}}
            for name in ("public_lookup", "admin_reset_key")
        ]}
    elif method == "tools/call" and payload.get("params", {}).get("name") in {
        "public_lookup", "admin_reset_key"
    }:
        result = {"content": [{"type": "text", "text": "Simulated action complete"}], "isError": False}
    else:
        return {**response, "error": {"code": -32601, "message": "Method or tool not found"}}
    return {**response, "result": result}


@app.post("/{target}/v1/chat/completions")
async def completion_mock(target: str, request: Request):
    payload = await request.json()
    mode = os.getenv("QUILR_MOCK_PRIMARY_MODE", "ok") if target == "primary" else "ok"
    if mode == "429":
        return JSONResponse({"error": "Mock provider rate limit"}, status_code=429)
    if mode == "timeout":
        await asyncio.sleep(4)

    if payload.get("stream"):
        async def generate():
            for chunk in (
                "Synthetic test: ", "email demo@", "example.com, SSN 000-",
                "00-0000, card 4111 1111 ", "1111 1111.",
            ):
                yield "data: " + json.dumps({"choices": [{"delta": {"content": chunk}}]}) + "\n\n"
                await asyncio.sleep(0.03)
            yield "data: [DONE]\n\n"
        return StreamingResponse(generate(), media_type="text/event-stream")
    return {"choices": [{"message": {"content": f"Synthetic completion from {target}."}}]}
