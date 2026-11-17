"""Run a bounded session, save it permanently, and resume on another runner."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

import aiohttp

from bsc_lifecycle.archive import Github, publish, restore
from bsc_lifecycle.collector import Collector
from bsc_lifecycle.network import RPC_URLS, Reader, ReadError
from bsc_lifecycle.store import Journal, Store, atomic, encoded, sha


def provenance() -> dict[str, Any]:
    root = Path(__file__).parent
    return {
        "source_commit": os.environ.get("GITHUB_SHA"),
        "python": sys.version,
        "files": {p.name: sha(p.read_bytes()) for p in sorted(root.iterdir()) if p.suffix in {".py", ".json"}},
        "purpose": "raw_data_collection_not_strategy_validation",
        "wallet_access": False,
    }


async def run(args: argparse.Namespace) -> dict[str, Any]:
    root = args.out
    root.mkdir(parents=True, exist_ok=True)
    archive = Github(args.archive_repo) if args.archive_repo else None
    database = root / "state.sqlite"
    if archive:
        restore(archive, database, bootstrap=args.bootstrap)
    store = Store(database)
    store.set("last_session", {"status": "RUNNING", "at": time.time()})
    store.db.commit()
    journal = Journal(root)
    origin = provenance()
    journal.write(
        "session",
        {
            "provenance": origin,
            "started_at": time.time(),
            "minutes": args.minutes,
            "restored": store.get("initial_block") is not None,
        },
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    urls = tuple(u.strip() for u in os.environ.get("BSC_RPC_URLS", "").split(",") if u.strip()) or RPC_URLS
    result: dict[str, Any] = {}
    async with aiohttp.ClientSession(trust_env=args.trust_env, timeout=aiohttp.ClientTimeout(total=30)) as session:
        reader = Reader(session, journal, urls)
        collector = Collector(store, journal, reader)
        deadline = time.monotonic() + args.minutes * 60
        checkpoint_at = time.monotonic() + args.checkpoint_seconds
        failures = 0
        try:
            if store.get("initial_block") is None:
                if not args.bootstrap:
                    raise ValueError("empty state requires --bootstrap")
                head = await reader.block("finalized")
                initial = max(1, int(head["number"], 16) - args.lookback_blocks)
                anchor = await reader.block(initial)
                journal.write(
                    "initialization",
                    {"initial_block": initial + 1, "anchor": anchor["hash"], "lookback_is_historical_not_live": True},
                )
                journal.flush()
                store.initialize(initial, anchor["hash"])
            while time.monotonic() < deadline and not stop.is_set():
                try:
                    await collector.cycle()
                    failures = 0
                except ReadError as exc:
                    failures += 1
                    store.quality("cycle_read_failure")
                    journal.write("cycle_failure", {"error": str(exc), "consecutive": failures})
                    store.db.commit()
                    if failures == 12:
                        journal.write("source_degraded", {"at": time.time(), "consecutive": failures, "cursor_preserved": True})
                    # Keep bounded collector session alive; never advance a failed stream.
                    # Subsequent polling cycles retry independently; archives still publish.
                status = store.status()
                atomic(root / "status.json", encoded(status))
                print(
                    json.dumps(
                        {
                            "tokens": status["tokens"],
                            "protocol_cursor": status["protocol_cursor"],
                            "oldest_entity_cursor": status["oldest_entity_cursor"],
                            "cycles": collector.cycles,
                            "read_failures": failures,
                        }
                    ),
                    flush=True,
                )
                if archive and time.monotonic() >= checkpoint_at:
                    result = publish(store, journal, root, archive, origin)
                    checkpoint_at = time.monotonic() + args.checkpoint_seconds
                    print(json.dumps({"published": result["asset"], "tokens": result["status"]["tokens"]}), flush=True)
                remaining = min(20, max(0, deadline - time.monotonic()))
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=remaining)
        except BaseException as exc:
            journal.write("session_failure", {"error_type": type(exc).__name__, "at": time.time()})
            store.set("last_session", {"status": "FAILED", "error_type": type(exc).__name__, "at": time.time()})
            raise
        else:
            store.set(
                "last_session",
                {
                    "status": "DEGRADED" if failures else ("STOPPED" if stop.is_set() else "COMPLETED"),
                    "at": time.time(),
                    "consecutive_rpc_failures": failures,
                    "cursor_preserved": True,
                },
            )
        finally:
            store.db.commit()
            journal.flush()
            atomic(root / "status.json", encoded(store.status()))
            if archive and store.get("initial_block") is not None:
                result = publish(store, journal, root, archive, origin)
            journal.file.close()
            store.db.close()
    return result or json.loads((root / "status.json").read_text())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--minutes", type=float, default=175)
    parser.add_argument("--lookback-blocks", type=int, default=4000)
    parser.add_argument("--checkpoint-seconds", type=int, default=600)
    parser.add_argument("--archive-repo")
    parser.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--trust-env", action="store_true")
    args = parser.parse_args()
    if not 0 < args.minutes <= 300 or not 1 <= args.lookback_blocks <= 100000:
        parser.error("invalid duration or lookback")
    if not 60 <= args.checkpoint_seconds <= 1800:
        parser.error("checkpoint interval must be 60..1800 seconds")
    print(json.dumps(asyncio.run(run(args)), sort_keys=True))


if __name__ == "__main__":
    main()
