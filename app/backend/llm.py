"""Foundation Model API helpers (OpenAI-compatible chat completions on Model Serving)."""
from __future__ import annotations

import asyncio

import httpx

from . import config

_client: httpx.AsyncClient | None = None


def client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=60)
    return _client


async def chat(endpoint: str, messages: list[dict], tools: list[dict] | None = None, max_tokens: int = 800,
               temperature: float = 0.2) -> dict:
    w = config.ws()
    headers = await asyncio.to_thread(w.config.authenticate)
    body = {"messages": messages, "max_tokens": max_tokens, "temperature": temperature}
    if tools:
        body["tools"] = tools
    r = await client().post(f"{w.config.host}/serving-endpoints/{endpoint}/invocations", json=body, headers=headers)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]


def text_of(msg: dict) -> str:
    """Message content as text (some endpoints return a list of content blocks)."""
    c = msg.get("content")
    if isinstance(c, list):
        return " ".join(b.get("text", "") for b in c if isinstance(b, dict))
    return c or ""
