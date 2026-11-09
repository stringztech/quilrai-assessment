import asyncio
import json
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from quilr_gateway.app import Config, create_app
from quilr_gateway.limiter import TokenWindow
from quilr_gateway.providers import Provider
from quilr_gateway.redaction import OVERLAP, StreamRedactor


def test_stdio_protocol_and_strict_validation():
    def request(i, method, params=None):
        result = {"jsonrpc": "2.0", "id": i, "method": method}
        if params is not None:
            result["params"] = params
        return result

    cases = [
        request(2, "tools/list"),
        request(3, "tools/call", {"name": "get_customer_record", "arguments": {"customer_id": "CUST-00001"}}),
        request(4, "tools/call", {"name": "get_customer_record", "arguments": {"customer_id": "CUST-00001\n"}}),
        request(5, "tools/call", {"name": "trigger_refund", "arguments": {"customer_id": "CUST-00001", "amount": "2.5", "reason": "Duplicate charge"}}),
        request(6, "tools/call", {"name": "trigger_refund", "arguments": {"customer_id": "CUST-00001", "amount": 2.5, "reason": "Duplicate charge"}}),
        request(7, "tools/call", {"name": "trigger_refund", "arguments": {"customer_id": "CUST-00001", "amount": 0, "reason": "Duplicate charge"}}),
        request(8, "tools/call", {"name": "trigger_refund", "arguments": {"customer_id": "CUST-00001", "amount": 1, "reason": "     short    "}}),
        request(9, "tools/call", {"name": "get_customer_record", "arguments": {"customer_id": "CUST-00001", "admin": True}}),
    ]
    with subprocess.Popen(
        [sys.executable, "-m", "quilr_gateway.mcp_server"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        cwd=Path(__file__).resolve().parents[1],
    ) as process:
        def send(message):
            process.stdin.write(json.dumps(message) + "\n")
            process.stdin.flush()

        def receive():
            return json.loads(process.stdout.readline())

        send(request(1, "initialize", {
            "protocolVersion": "2025-11-25", "capabilities": {},
            "clientInfo": {"name": "test", "version": "1.0"},
        }))
        assert receive()["result"]["serverInfo"]["name"] == "customer-service"
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        responses = {}
        for case in cases:
            send(case)
            answer = receive()
            responses[answer["id"]] = answer
        process.stdin.close()
        assert process.wait(timeout=10) == 0
        assert process.stdout.read() == ""
        assert "Starting customer MCP server" in process.stderr.read()

    assert {t["name"] for t in responses[2]["result"]["tools"]} == {
        "get_customer_record", "trigger_refund"
    }
    assert responses[3]["result"]["structuredContent"]["status"] == "active"
    assert responses[6]["result"]["structuredContent"]["status"] == "simulated"
    assert all(responses[i]["error"]["code"] == -32602 for i in (4, 5, 7, 8, 9))


@pytest.fixture
def config(tmp_path):
    return Config(
        "http://mock/mcp", {"viewer": "viewer-demo", "admin": "admin-demo"},
        {"tenant": "tenant-demo", "other": "other-demo"},
        Provider("http://mock/primary", "primary"),
        Provider("http://mock/backup", "backup"),
        tmp_path / "limits.sqlite3",
    )


def test_mcp_proxy_preserves_wire_and_blocks_viewer(config):
    calls = []

    def upstream(request):
        calls.append(request)
        body = json.loads(request.content)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": {"tools": []}})

    viewer = {"Authorization": "Bearer viewer-demo"}
    admin = {"Authorization": "Bearer admin-demo"}
    call = {"jsonrpc": "2.0", "id": "rpc-1", "method": "tools/call", "params": {"name": "admin_reset_key"}}
    listing = {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
    with TestClient(create_app(config, transport=httpx.MockTransport(upstream))) as client:
        assert client.post("/mcp", json=call).status_code == 401
        denied = client.post("/mcp", json=call, headers=viewer).json()
        assert denied == {"jsonrpc": "2.0", "id": "rpc-1", "error": {
            "code": -32001, "message": "Unauthorized Tool Call"
        }}
        assert client.post("/mcp", json=[call], headers=viewer).json()["error"]["code"] == -32600
        assert client.post("/mcp", content=b"{", headers=viewer).json()["error"]["code"] == -32700
        duplicate = b'{"jsonrpc":"2.0","id":3,"method":"tools/list","method":"tools/call"}'
        assert client.post("/mcp", content=duplicate, headers=viewer).json()["error"]["code"] == -32700
        assert not calls
        raw = json.dumps(listing, indent=2).encode()
        assert client.post("/mcp", content=raw, headers=viewer).json()["id"] == 2
        assert calls[0].content == raw
        assert "authorization" not in calls[0].headers
        assert client.post("/mcp", json=call, headers=admin).json()["id"] == "rpc-1"
        assert len(calls) == 2


@pytest.mark.parametrize("pii", ["demo@example.com", "000-00-0000", "4111 1111 1111 1111"])
def test_redaction_at_every_two_chunk_boundary(pii):
    source = f"Before: {pii}. After."
    for i in range(len(source) + 1):
        redactor = StreamRedactor()
        first = "".join(redactor.feed(source[:i]))
        second = "".join(redactor.feed(source[i:]))
        assert first + second + redactor.finish() == "Before: [REDACTED]. After."


def test_redactor_releases_safe_prefix_and_bounds_pending():
    redactor = StreamRedactor()
    assert "".join(redactor.feed("Safe prefix: ")) == "Safe prefix: "
    assert "".join(redactor.feed("demo@exa")) == ""
    assert "".join(redactor.feed("mple.com, ")) == "[REDACTED], "
    assert "".join(redactor.feed("x" * 5000)) + redactor.finish() == "x" * 5000
    assert len(redactor.pending) <= OVERLAP


class Chunks(httpx.AsyncByteStream):
    def __init__(self, raw):
        self.raw = raw
        self.closed = False

    async def __aiter__(self):
        for i in range(0, len(self.raw), 3):
            yield self.raw[i:i + 3]

    async def aclose(self):
        self.closed = True


def test_stream_handles_wire_and_model_boundaries(config):
    wire = b"".join(
        ("data: " + json.dumps({"choices": [{"delta": {"content": text}}]}, ensure_ascii=False) + "\r\n\r\n").encode()
        for text in ("Café: ", "demo@", "example.com; ", "000-00-", "0000.")
    ) + b"data: [DONE]\n\n"
    stream = Chunks(wire)

    def upstream(request):
        assert json.loads(request.content)["stream"] is True
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=stream)

    with TestClient(create_app(config, transport=httpx.MockTransport(upstream))) as client:
        response = client.post("/llm/stream", json={"prompt": "Demo"}, headers={"Authorization": "Bearer tenant-demo"})
        frames = response.text.splitlines()
    safe = "".join(json.loads(line[6:])["delta"] for line in frames if line.startswith("data: {"))
    assert safe == "Café: [REDACTED]; [REDACTED]."
    assert "data: [DONE]" in frames
    assert stream.closed


@pytest.mark.parametrize("mode", ["429", "timeout", "both_fail", "primary_fatal"])
def test_fallback_and_sanitized_error(config, mode):
    calls = []

    async def upstream(request):
        calls.append(request.url.path)
        if request.url.path == "/primary":
            if mode == "timeout":
                await asyncio.sleep(10)
            return httpx.Response(500 if mode == "primary_fatal" else 429, text="provider stack secret")
        if mode == "both_fail":
            return httpx.Response(500, text="backup stack secret")
        return httpx.Response(200, json={"choices": [{"message": {"content": "Safe answer."}}]})

    with TestClient(create_app(config, transport=httpx.MockTransport(upstream))) as client:
        response = client.post("/llm/complete", json={"prompt": "Demo", "max_tokens": 20}, headers={"Authorization": "Bearer tenant-demo"})
    assert calls == (["/primary"] if mode == "primary_fatal" else ["/primary", "/backup"])
    if mode in ("both_fail", "primary_fatal"):
        assert response.status_code == 502
        assert response.json()["error"]["code"] == "upstream_unavailable"
        assert "secret" not in response.text
    else:
        assert response.json() == {"text": "Safe answer.", "provider": "backup"}
    with sqlite3.connect(config.db_path) as db:
        assert db.execute("SELECT tokens FROM admissions").fetchall() == [(56,)]


def test_sqlite_window_concurrency_and_expiry(tmp_path):
    async def exercise():
        path = tmp_path / "limits.sqlite3"
        window = TokenWindow(path, limit=100)
        window.initialize()
        # Two instances share the same disk budget, as separate workers would.
        other_worker = TokenWindow(path, limit=100)
        windows = [window, other_worker] * 5
        results = await asyncio.gather(*(w.reserve("a", 30) for w in windows))
        assert sum(results) == 3
        assert await window.reserve("b", 100)
        assert not await other_worker.reserve("a", 11)
        with sqlite3.connect(path) as db:
            db.execute("UPDATE admissions SET at = ? WHERE tenant = 'a'", (time.time() - 61,))
        assert await window.reserve("a", 100)
        assert not await other_worker.reserve("b", 1)

    asyncio.run(exercise())
