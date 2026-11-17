"""Continuous, resumable census of protocol logs and observed token/pool lifecycles."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from bsc_lifecycle.codec import HASH
from bsc_lifecycle.network import Oversize, Reader, ReadError
from bsc_lifecycle.store import Journal, Store, encoded, sha


class IntegrityError(Exception):
    """Stop on incompatible chain evidence. Never silently reset a cursor."""


def validate_logs(logs: Any, members: dict[str, str], start: int, end: int) -> list[dict[str, Any]]:
    if not isinstance(logs, list):
        raise IntegrityError("RPC logs are not a list")
    if len(logs) >= 5000:
        raise Oversize("possible provider result cap")
    seen: dict[str, dict[str, Any]] = {}
    for log in logs:
        if not isinstance(log, dict):
            raise IntegrityError("invalid log")
        try:
            if log.get("removed") is not False or log["address"].lower() not in members:
                raise IntegrityError("removed or unrelated log")
            if not start <= int(log["blockNumber"], 16) <= end:
                raise IntegrityError("log outside requested range")
            for key in ("blockHash", "transactionHash"):
                if not HASH.fullmatch(log[key]):
                    raise IntegrityError("invalid log identity")
            if any(not HASH.fullmatch(t) for t in log["topics"]):
                raise IntegrityError("invalid topic")
            if int(log["logIndex"], 16) < 0 or not log["data"].startswith("0x"):
                raise IntegrityError("invalid log payload")
            bytes.fromhex(log["data"][2:])
            key = log["blockHash"] + ":" + log["logIndex"]
            if key in seen and seen[key] != log:
                raise IntegrityError("conflicting duplicate log")
            seen[key] = log
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise IntegrityError("malformed log envelope") from exc
    return list(seen.values())


class Collector:
    def __init__(self, store: Store, journal: Journal, reader: Reader) -> None:
        self.store, self.journal, self.reader = store, journal, reader
        self.cycles = 0
        self.last_enrichment = 0.0

    async def scan(self, name: str, head: int, *, span: int = 500) -> bool:
        row = self.store.db.execute("SELECT * FROM streams WHERE name=?", (name,)).fetchone()
        cursor = int(row["cursor"])
        if cursor >= head:
            return False
        if row["hash"]:
            previous = await self.reader.block(cursor)
            if previous["hash"] != row["hash"]:
                raise IntegrityError("committed block hash changed; quarantine and restore required")
        start, end = cursor + 1, min(head, cursor + span)
        members = self.store.members(name)
        while True:
            before = await self.reader.block(end)
            try:
                logs = []
                addresses = sorted(members)
                # PublicNode documents a ten-address request limit; never send a wider filter.
                for offset in range(0, len(addresses), 10):
                    subset = {a: members[a] for a in addresses[offset : offset + 10]}
                    raw = await self.reader.rpc(
                        "eth_getLogs", [{"fromBlock": hex(start), "toBlock": hex(end), "address": list(subset)}]
                    )
                    logs.extend(validate_logs(raw, subset, start, end))
                break
            except Oversize:
                if end == start:
                    raise IntegrityError("one block exceeds provider limits; cursor retained") from None
                end = (start + end) // 2
        # Preserve original bytes in the journal. A derived timestamp is explicitly marked.
        timestamp_headers: dict[int, dict[str, Any]] = {}
        for log in logs:
            block = int(log["blockNumber"], 16)
            if block == end and log["blockHash"] != before["hash"]:
                raise IntegrityError("log and boundary header disagree")
            if not log.get("blockTimestamp"):
                if block not in timestamp_headers:
                    timestamp_headers[block] = await self.reader.block(block)
                header = timestamp_headers[block]
                if log["blockHash"] != header["hash"]:
                    raise IntegrityError("timestamp header disagrees")
                log["blockTimestamp"] = header["timestamp"]
                log["timestamp_source"] = "header_read"
            if int(log["blockTimestamp"], 16) > int(before["timestamp"], 16):
                raise IntegrityError("log timestamp after boundary")
        after = await self.reader.block(end)
        if before["hash"] != after["hash"]:
            raise IntegrityError("range changed during read")
        self.journal.write(
            "coverage",
            {
                "stream": name,
                "from_block": start,
                "to_block": end,
                "boundary_hash": after["hash"],
                "boundary_time": int(after["timestamp"], 16),
                "addresses": sorted(members),
                "unique_logs": len(logs),
            },
        )
        self.journal.flush()  # raw bytes durable before the database cursor advances
        self.store.commit_range(name, end, after["hash"], logs)
        self.store.set("covered_time:" + name, int(after["timestamp"], 16))
        self.store.db.commit()
        return True

    async def enrich(self) -> None:
        now = time.time()
        # All observed tokens are scheduled; no price/return/volume-based survival filter.
        rows = self.store.db.execute(
            """SELECT address FROM tokens WHERE
            market_at < ? - CASE WHEN birth_ts>? THEN 60 WHEN birth_ts>? THEN 300 ELSE 1800 END
            ORDER BY market_at,address LIMIT 30""",
            (now, now - 7200, now - 86400),
        ).fetchall()
        tokens = [r[0] for r in rows]
        if tokens:
            try:
                data = await self.reader.request(
                    "dexscreener", "https://api.dexscreener.com/tokens/v1/bsc/" + ",".join(tokens)
                )
                if not isinstance(data, list):
                    raise ReadError("DexScreener shape mismatch")
                self.journal.write(
                    "market_batch",
                    {
                        "tokens_requested": tokens,
                        "markets_returned": len(data),
                        "payload_sha256": sha(encoded(data)),
                        "provider_event_time": None,
                    },
                )
            except ReadError:
                self.store.quality("market_read_failure")
            with self.store.db:
                self.store.db.executemany("UPDATE tokens SET market_at=? WHERE address=?", [(now, t) for t in tokens])
        rows = self.store.db.execute(
            "SELECT address FROM tokens WHERE security_at<? ORDER BY security_at,address LIMIT 10", (now - 1800,)
        ).fetchall()
        tokens = [r[0] for r in rows]
        for token in tokens:
            # The unauthenticated endpoint returned only one token for a ten-token batch.
            # Read each token explicitly and stay below 30 requests/minute, including retries.
            await asyncio.sleep(3.1)
            try:
                data = await self.reader.request(
                    "goplus",
                    "https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses=" + token,
                )
                if not isinstance(data, dict) or data.get("code") != 1 or not isinstance(data.get("result"), dict):
                    raise ReadError("GoPlus shape mismatch")
                info = data["result"].get(token)
                self.journal.write(
                    "security_observation",
                    {
                        "token": token,
                        "present": isinstance(info, dict),
                        "provider_event_time": None,
                        "basis": "provider_report_not_execution_proof",
                    },
                )
                if isinstance(info, dict):
                    self.store.facts("tokens", token, {"security": info, "security_received_at": time.time()})
                else:
                    self.store.quality("security_token_missing")
            except ReadError:
                self.store.quality("security_read_failure")
                with self.store.db:
                    self.store.db.execute("UPDATE tokens SET security_at=? WHERE address=?", (now, token))
                break  # Do not mark unattempted tokens as examined when the provider fails.
            with self.store.db:
                self.store.db.execute("UPDATE tokens SET security_at=? WHERE address=?", (now, token))
        self.journal.flush()

    async def cycle(self) -> None:
        head = await self.reader.block("finalized")
        age = time.time() - int(head["timestamp"], 16)
        if not -10 <= age <= 300:
            raise ReadError("finalized head stale or future dated")
        height = int(head["number"], 16)
        self.store.set(
            "last_finalized_head",
            {
                "block": height,
                "timestamp": int(head["timestamp"], 16),
                "hash": head["hash"],
                "received_at": time.time(),
            },
        )
        for _ in range(4):
            if not await self.scan("protocols", height):
                break
        # Follow only through the discovered frontier, including each birth block.
        frontier = self.store.db.execute("SELECT cursor FROM streams WHERE name='protocols'").fetchone()[0]
        pending = self.store.db.execute(
            """SELECT name FROM streams WHERE kind='entities' AND cursor<?
            ORDER BY updated,start_block LIMIT 12""",
            (frontier,),
        ).fetchall()
        for row in pending:
            # Old, quiet entities can catch up hours without thousands of empty small calls.
            # Dense responses are recursively split and no cursor skips an incomplete range.
            await self.scan(row[0], frontier, span=50_000)
        if time.time() - self.last_enrichment >= 30:
            await self.enrich()
            self.last_enrichment = time.time()
        self.cycles += 1
        self.store.set("last_cycle", {"at": time.time(), "cycles_this_process": self.cycles})
        self.store.db.commit()
