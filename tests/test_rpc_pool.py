"""Contract tests for rotating read-only BSC RPC transport (no network access)."""
from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any

import pytest

from bsc_lifecycle.rpc_pool import RangeLimitError, RPCPool, TransientRPCError

URLS = (
    "https://rpc-a.example/key1",
    "https://rpc-b.example/key2",
    "https://rpc-c.example/key3",
)


class Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        assert delay >= 0
        self.sleeps.append(delay)
        self.now += delay


def run(coro: Any) -> Any:
    return asyncio.run(coro)


def test_rotates_across_providers_even_on_success() -> None:
    calls: list[tuple[str, str]] = []

    async def execute(url: str, method: str, params: list[Any]) -> Any:
        calls.append((url, method))
        return "0x38" if method == "eth_chainId" else url

    pool = RPCPool(URLS, execute)
    assert [run(pool.read("eth_blockNumber", [])) for _ in range(4)] == [*URLS, URLS[0]]
    assert [url for url, method in calls if method == "eth_chainId"] == list(URLS)
    assert sum(e["successes"] for e in pool.status()) == 4
    assert all("key" not in str(e) for e in pool.status())


def test_rate_limit_fails_over_immediately_and_respects_retry_after() -> None:
    clock = Clock()
    attempted: list[str] = []

    async def execute(url: str, method: str, params: list[Any]) -> Any:
        if method == "eth_chainId":
            return "0x38"
        attempted.append(url)
        if url == URLS[0]:
            raise TransientRPCError("HTTP 429", retry_after=40)
        return {"number": "0x65"}

    pool = RPCPool(URLS, execute, clock=clock, sleep=clock.sleep)
    assert run(pool.read("eth_getBlockByNumber", ["0x65", False])) == {"number": "0x65"}
    assert attempted == [URLS[0], URLS[1]]
    assert pool.endpoints[0].ready_at == 40
    assert not clock.sleeps


def test_cooldown_recovery() -> None:
    clock = Clock()
    counts: dict[str, int] = defaultdict(int)

    async def execute(url: str, method: str, params: list[Any]) -> Any:
        if method == "eth_chainId":
            return "0x38"
        counts[url] += 1
        if counts[url] == 1:
            raise TransientRPCError("timeout")
        return "recovered"

    pool = RPCPool(URLS, execute, clock=clock, sleep=clock.sleep, retry_budget_sec=5)
    assert run(pool.read("eth_getLogs", [])) == "recovered"
    assert clock.sleeps
    assert pool.endpoints[0].failures == 0


def test_wrong_chain_never_serves_requested_data() -> None:
    calls: list[tuple[str, str]] = []

    async def execute(url: str, method: str, params: list[Any]) -> Any:
        calls.append((url, method))
        return ("0x1" if url == URLS[0] else "0x38") if method == "eth_chainId" else "data"

    pool = RPCPool(URLS, execute)
    assert run(pool.read("eth_getLogs", [])) == "data"
    assert pool.endpoints[0].excluded
    assert (URLS[0], "eth_getLogs") not in calls


def test_all_providers_unavailable_does_not_make_up_data() -> None:
    clock = Clock()
    attempts: list[str] = []

    async def execute(url: str, method: str, params: list[Any]) -> Any:
        attempts.append(url)
        raise TransientRPCError("offline", retry_after=20)

    pool = RPCPool(URLS, execute, clock=clock, sleep=clock.sleep, retry_budget_sec=3)
    with pytest.raises(TransientRPCError):
        run(pool.read("eth_getLogs", []))
    assert len(attempts) == 3
    assert clock.now == 3


def test_oversize_fails_closed_for_collector_to_split() -> None:
    async def execute(url: str, method: str, params: list[Any]) -> Any:
        if method == "eth_chainId":
            return "0x38"
        raise RangeLimitError("range too large")

    with pytest.raises(RangeLimitError, match="split"):
        run(RPCPool(URLS, execute).read("eth_getLogs", []))


@pytest.mark.parametrize(
    "invalid",
    [(), (URLS[0], URLS[0]), (URLS[0], URLS[1]), ("http://bad.example",), ("https://user:password@bad.example",)],
)
def test_rejects_unsafe_endpoint_configuration(invalid: tuple[str, ...]) -> None:
    async def execute(url: str, method: str, params: list[Any]) -> Any:
        return None

    with pytest.raises(ValueError, match="RPC endpoints|RPC endpoint|at least 3"):
        RPCPool(invalid, execute)


def test_programming_errors_are_not_misclassified_as_network_failures() -> None:
    async def execute(url: str, method: str, params: list[Any]) -> Any:
        raise TypeError("parser bug")

    with pytest.raises(TypeError, match="parser bug"):
        run(RPCPool(URLS, execute).read("eth_getLogs", []))
