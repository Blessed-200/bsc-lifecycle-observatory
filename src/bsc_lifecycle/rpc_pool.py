"""Read-only BSC RPC failover. A provider outage never fabricates a chain response.

The calling collector remains responsible for durable, contiguous per-stream cursors.
This module selects providers and retries *reads* only. It must never sign or send TXs.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from urllib.parse import urlsplit
from typing import Any


class TransientRPCError(Exception):
    """Transport/rate-limit/unknown read failure. Safe to retry on another RPC."""

    def __init__(self, message: str = "RPC read unavailable", *, retry_after: float = 0.0) -> None:
        super().__init__(message)
        self.retry_after = max(0.0, float(retry_after))


class PermanentRPCError(Exception):
    """Wrong chain or unauthorized endpoint: exclude until process restart."""


class RangeLimitError(Exception):
    """At least one RPC cannot serve this query range; caller must split it."""


@dataclass(slots=True)
class Endpoint:
    url: str
    verified: bool = False
    excluded: bool = False
    failures: int = 0
    successes: int = 0
    ready_at: float = 0.0


RPCFn = Callable[[str, str, list[Any]], Awaitable[Any]]
SleepFn = Callable[[float], Awaitable[None]]


class RPCPool:
    """Round-robin, independent circuits, bounded exponential backoff, BSC chain-ID check.

    A finite call budget ensures bounded collector sessions and fail-closed behavior.
    The next polling cycle can retry without advancing any block cursor.
    """

    def __init__(
        self,
        urls: Sequence[str],
        execute: RPCFn,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: SleepFn = asyncio.sleep,
        retry_budget_sec: float = 15.0,
        base_backoff_sec: float = 1.0,
        max_backoff_sec: float = 60.0,
    ) -> None:
        if len(urls) < 3 or len(set(urls)) != len(urls):
            raise ValueError("at least 3 distinct RPC endpoints required")
        for url in urls:
            parts = urlsplit(url)
            if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
                raise ValueError("RPC endpoints must use HTTPS without URL userinfo")
        if not (0 < retry_budget_sec <= 300 and 0 < base_backoff_sec <= max_backoff_sec <= 3600):
            raise ValueError("invalid retry/backoff configuration")
        self.endpoints = [Endpoint(url) for url in urls]
        self.execute = execute
        self.clock, self.sleep = clock, sleep
        self.retry_budget_sec = retry_budget_sec
        self.base_backoff_sec = base_backoff_sec
        self.max_backoff_sec = max_backoff_sec
        self._cursor = 0
        self._turn_lock = asyncio.Lock()

    def status(self) -> list[dict[str, Any]]:
        """Endpoint telemetry safe for status: host only, never API keys or URL path."""
        return [
            {
                "host": urlsplit(node.url).hostname,
                "verified": node.verified,
                "excluded": node.excluded,
                "failures": node.failures,
                "successes": node.successes,
                "cooldown_seconds": round(max(0.0, node.ready_at - self.clock()), 2),
            }
            for node in self.endpoints
        ]

    async def read(self, method: str, params: list[Any]) -> Any:
        # Ordering state is allocated atomically even if concurrent collector jobs ask for reads.
        async with self._turn_lock:
            start = self._cursor
            self._cursor = (self._cursor + 1) % len(self.endpoints)
        deadline = self.clock() + self.retry_budget_sec
        range_limited = False
        last_error = "all RPC endpoints unavailable"
        while True:
            now = self.clock()
            if now >= deadline:
                break
            for offset in range(len(self.endpoints)):
                node = self.endpoints[(start + offset) % len(self.endpoints)]
                if node.excluded or node.ready_at > self.clock():
                    continue
                try:
                    if not node.verified:
                        chain_id = await self.execute(node.url, "eth_chainId", [])
                        if chain_id != "0x38":
                            raise PermanentRPCError("endpoint returned unexpected chain ID")
                        node.verified = True
                    value = await self.execute(node.url, method, params)
                    node.failures = 0
                    node.successes += 1
                    return value
                except RangeLimitError:
                    # Switching to a larger-capacity provider may save a split.
                    range_limited = True
                    continue
                except PermanentRPCError as exc:
                    node.excluded = True
                    last_error = str(exc)
                except TransientRPCError as exc:
                    node.failures += 1
                    delay = min(self.max_backoff_sec, self.base_backoff_sec * 2 ** min(node.failures - 1, 12))
                    node.ready_at = self.clock() + max(delay, exc.retry_after)
                    last_error = str(exc)
            # A range-limited response is explicit evidence to split, never a successful empty range.
            if range_limited:
                raise RangeLimitError("RPC query too large; split range without advancing cursor")
            available = [n.ready_at for n in self.endpoints if not n.excluded]
            if not available:
                break
            remaining = deadline - self.clock()
            if remaining <= 0:
                break
            until_ready = max(0.0, min(available) - self.clock())
            # Guard against a busy loop when clocks advance only microscopically.
            await self.sleep(min(remaining, max(0.05, until_ready)))
        raise TransientRPCError(last_error)
