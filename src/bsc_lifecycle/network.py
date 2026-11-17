"""Bounded public reads, original response bytes, backoff and explicit provider failures."""

from __future__ import annotations

import asyncio
import base64
import json
import time
from typing import Any
from urllib.parse import urlsplit

import aiohttp

from bsc_lifecycle.store import Journal, sha

RPC_URLS = ("https://bsc-rpc.publicnode.com", "https://bsc-dataseed.bnbchain.org", "https://bsc-dataseed1.defibit.io")
READ_METHODS = frozenset(
    {
        "eth_chainId",
        "eth_getBlockByNumber",
        "eth_getLogs",
        "eth_call",
        "eth_getCode",
        "eth_getTransactionByHash",
        "eth_getTransactionReceipt",
    }
)


class ReadError(Exception):
    """A read failed; never advance a cursor or turn an unknown into a zero."""


class Oversize(ReadError):
    """Split log ranges; do not silently accept a capped response."""


class Reader:
    def __init__(self, session: aiohttp.ClientSession, journal: Journal, urls: tuple[str, ...] = RPC_URLS) -> None:
        for url in urls:
            if urlsplit(url).scheme != "https" or not urlsplit(url).hostname:
                raise ValueError("RPC endpoint must use HTTPS")
        if not urls:
            raise ValueError("no RPC endpoints")
        self.session, self.journal, self.urls = session, journal, urls
        self.verified: set[str] = set()
        self.cooldown: dict[str, float] = {}
        self.next_rpc = 0.0
        self.serial = 0

    async def request(self, source: str, url: str, payload: dict[str, Any] | None = None) -> Any:
        if time.time() < self.cooldown.get(source, 0):
            raise ReadError("provider cooling down")
        started = time.time()
        method = "POST" if payload is not None else "GET"
        record: dict[str, Any] = {
            "source": source,
            "host": urlsplit(url).hostname,
            "request_started_at": started,
            "method": method,
            "request": payload,
            "url": url if payload is None else None,
        }
        try:
            async with self.session.request(method, url, json=payload, allow_redirects=False) as response:
                limit = 16_000_000
                chunks, size = [], 0
                async for chunk in response.content.iter_chunked(65536):
                    chunks.append(chunk)
                    size += len(chunk)
                    if size > limit:
                        break
                body = b"".join(chunks)
                record.update(
                    {
                        "status": response.status,
                        "received_at": time.time(),
                        "body_b64": base64.b64encode(body).decode(),
                        "body_sha256": sha(body),
                        "truncated": size > limit,
                    }
                )
                self.journal.write("http", record)
                if size > limit:
                    raise Oversize("response exceeds raw-body bound; split query")
                if response.status == 429:
                    retry = response.headers.get("Retry-After", "60")
                    delay = max(60, min(3600, int(retry))) if retry.isdigit() else 60
                    self.cooldown[source] = time.time() + delay
                    raise ReadError("HTTP 429; Retry-After respected")
                if response.status in {401, 403}:
                    self.cooldown[source] = time.time() + 3600
                if response.status != 200:
                    raise ReadError(f"HTTP {response.status}")
                try:
                    return json.loads(body)
                except (ValueError, UnicodeError) as exc:
                    raise ReadError("invalid JSON") from exc
        except (aiohttp.ClientError, TimeoutError) as exc:
            self.journal.write("http_failure", {**record, "error_type": type(exc).__name__})
            self.cooldown[source] = time.time() + 10
            raise ReadError("network read failed") from exc

    async def _one(self, url: str, method: str, params: list[Any]) -> Any:
        await asyncio.sleep(max(0, self.next_rpc - time.monotonic()))
        self.next_rpc = time.monotonic() + 0.35
        self.serial += 1
        source = "rpc-" + sha(url.encode())[:12]
        result = await self.request(
            source, url, {"jsonrpc": "2.0", "id": self.serial, "method": method, "params": params}
        )
        if not isinstance(result, dict) or result.get("id") != self.serial:
            raise ReadError("RPC identity mismatch")
        if result.get("error"):
            message = str(result["error"].get("message", "")).lower()
            if any(x in message for x in ("too many results", "response size", "block range")):
                raise Oversize("RPC range limit")
            self.cooldown[source] = time.time() + 60
            raise ReadError("RPC error; original response retained")
        if "result" not in result or result["result"] is None:
            raise ReadError("missing RPC result")
        return result["result"]

    async def rpc(self, method: str, params: list[Any]) -> Any:
        if method not in READ_METHODS:
            raise ValueError("RPC mutation forbidden")
        for url in self.urls:
            source = "rpc-" + sha(url.encode())[:12]
            if self.cooldown.get(source, 0) > time.time():
                continue
            try:
                if url not in self.verified:
                    if await self._one(url, "eth_chainId", []) != "0x38":
                        self.cooldown[source] = time.time() + 3600
                        raise ReadError("wrong chain")
                    self.verified.add(url)
                return await self._one(url, method, params)
            except Oversize:
                raise
            except ReadError:
                continue
        raise ReadError("all RPC providers unavailable or cooling down")

    async def block(self, number: int | str) -> dict[str, Any]:
        value = await self.rpc("eth_getBlockByNumber", [hex(number) if isinstance(number, int) else number, False])
        if not isinstance(value, dict) or not value.get("hash") or not value.get("number"):
            raise ReadError("invalid block header")
        if isinstance(number, int) and int(value["number"], 16) != number:
            raise ReadError("wrong block height")
        return dict(value)
