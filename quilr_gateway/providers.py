"""OpenAI-compatible completion and SSE adapter."""

import asyncio
import json
from dataclasses import dataclass

import httpx

MAX_SSE_EVENT_BYTES = 128 * 1024


@dataclass(frozen=True)
class Provider:
    url: str
    model: str
    key: str = ""

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.key}"} if self.key else {}

    def payload(self, prompt: str, max_tokens: int, *, stream: bool) -> dict:
        return {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "stream": stream,
        }


class UpstreamError(Exception):
    def __init__(self, code: str, *, retryable: bool = False):
        super().__init__(code)
        self.code = code
        self.retryable = retryable


async def complete(client: httpx.AsyncClient, provider: Provider, prompt: str, max_tokens: int) -> str:
    try:
        async with asyncio.timeout(3):
            response = await client.post(
                provider.url, headers=provider.headers(),
                json=provider.payload(prompt, max_tokens, stream=False),
            )
    except (TimeoutError, httpx.TimeoutException):
        raise UpstreamError("upstream_timeout", retryable=True) from None
    except httpx.HTTPError:
        raise UpstreamError("upstream_unavailable") from None

    if response.status_code == 429:
        raise UpstreamError("upstream_rate_limited", retryable=True)
    if not response.is_success:
        raise UpstreamError("upstream_unavailable")

    try:
        content = response.json()["choices"][0]["message"]["content"]
        if not isinstance(content, str):
            raise ValueError
        return content
    except (KeyError, IndexError, TypeError, ValueError):
        raise UpstreamError("invalid_upstream_response") from None


async def sse_data(response: httpx.Response):
    """Yield complete SSE data events, regardless of HTTP chunk boundaries."""
    line_buffer = bytearray()
    data = []
    event_bytes = 0
    async for chunk in response.aiter_bytes():
        segments = chunk.split(b"\n")
        for index, segment in enumerate(segments):
            if len(line_buffer) + len(segment) > MAX_SSE_EVENT_BYTES:
                raise UpstreamError("invalid_upstream_stream")
            line_buffer.extend(segment)
            if index == len(segments) - 1:
                continue
            line = bytes(line_buffer).rstrip(b"\r")
            line_buffer.clear()
            if not line:
                if data:
                    yield b"\n".join(data)
                    data.clear()
                event_bytes = 0
            elif line == b"data" or line.startswith(b"data:"):
                value = line.partition(b":")[2].removeprefix(b" ")
                event_bytes += len(value) + 1
                if event_bytes > MAX_SSE_EVENT_BYTES:
                    raise UpstreamError("invalid_upstream_stream")
                data.append(value)


async def text_deltas(response: httpx.Response):
    async for event in sse_data(response):
        if event == b"[DONE]":
            return
        try:
            packet = json.loads(event)
            if not isinstance(packet, dict) or "error" in packet:
                raise ValueError
            for choice in packet["choices"]:
                # The gateway requests a single choice and forwards text only.
                if choice.get("index", 0) != 0:
                    continue
                content = choice["delta"].get("content")
                if content is not None:
                    if not isinstance(content, str):
                        raise ValueError
                    yield content
        except (KeyError, TypeError, AttributeError, ValueError):
            raise UpstreamError("invalid_upstream_stream") from None
    raise UpstreamError("incomplete_upstream_stream")
