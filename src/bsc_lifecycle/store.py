"""Transactional cursors and derived state; original responses live in the journal."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from collections import Counter
from pathlib import Path
from typing import Any

from bsc_lifecycle.codec import CONTRACTS, ZERO, address, decode

CORE = frozenset(
    {
        ZERO,
        "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
        "0x55d398326f99059ff775485246999027b3197955",
        "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
        "0xe9e7cea3dedca5984780bafc599bd69add087d56",
    }
)


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".pending")
    with temp.open("wb") as out:
        out.write(data)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temp, path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class Journal:
    def __init__(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / f"raw-{time.time_ns()}.jsonl"
        self.file = self.path.open("ab")
        self.counts: Counter[str] = Counter()

    def write(self, kind: str, value: dict[str, Any]) -> None:
        self.file.write(encoded({"kind": kind, "received_at": time.time(), **value}) + b"\n")
        self.counts[kind] += 1

    def flush(self) -> None:
        self.file.flush()
        os.fsync(self.file.fileno())

    def rotate(self) -> Path:
        self.flush()
        old = self.path
        self.file.close()
        self.path = old.with_name(f"raw-{time.time_ns()}.jsonl")
        self.file = self.path.open("ab")
        return old


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS streams(name TEXT PRIMARY KEY,cursor INTEGER NOT NULL,hash TEXT,
                start_block INTEGER NOT NULL,kind TEXT NOT NULL,updated REAL NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS entities(address TEXT PRIMARY KEY,kind TEXT NOT NULL,
                stream TEXT NOT NULL,start_block INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS tokens(address TEXT PRIMARY KEY,platform TEXT,birth_block INTEGER,
                birth_ts INTEGER,creator TEXT,first_block INTEGER NOT NULL,facts TEXT NOT NULL DEFAULT '{}',
                market_at REAL NOT NULL DEFAULT 0,security_at REAL NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS pools(address TEXT PRIMARY KEY,kind TEXT NOT NULL,
                token0 TEXT NOT NULL,token1 TEXT NOT NULL,birth_block INTEGER NOT NULL,
                facts TEXT NOT NULL DEFAULT '{}');
            CREATE TABLE IF NOT EXISTS seen(id TEXT PRIMARY KEY,block INTEGER NOT NULL,event TEXT NOT NULL,
                source TEXT NOT NULL,token TEXT,ts INTEGER,stream TEXT NOT NULL DEFAULT 'legacy');
            CREATE TABLE IF NOT EXISTS event_counts(event TEXT PRIMARY KEY,count INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS balances(token TEXT NOT NULL,wallet TEXT NOT NULL,value TEXT NOT NULL,
                PRIMARY KEY(token,wallet));
            CREATE INDEX IF NOT EXISTS seen_time ON seen(ts,event);
            CREATE INDEX IF NOT EXISTS entity_stream ON entities(stream);
            CREATE TABLE IF NOT EXISTS quality(kind TEXT PRIMARY KEY,count INTEGER NOT NULL);
        """)
        version = self.get("version")
        if version not in (None, 1):
            raise ValueError("incompatible lifecycle state")
        self.set("version", 1)
        if "stream" not in {row[1] for row in self.db.execute("PRAGMA table_info(seen)")}:
            self.db.execute("ALTER TABLE seen ADD COLUMN stream TEXT NOT NULL DEFAULT 'legacy'")
        if not self.get("event_counts_initialized"):
            self.db.execute("INSERT OR IGNORE INTO event_counts SELECT event,count(*) FROM seen GROUP BY event")
            self.set("event_counts_initialized", True)
        self.db.commit()

    def get(self, key: str) -> Any:
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def set(self, key: str, value: Any) -> None:
        self.db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, encoded(value).decode()))

    def quality(self, kind: str) -> None:
        self.db.execute("INSERT INTO quality VALUES (?,1) ON CONFLICT(kind) DO UPDATE SET count=count+1", (kind,))

    def initialize(self, block: int, block_hash: str) -> None:
        if self.get("initial_block") is not None:
            raise ValueError("state already initialized")
        self.set("initial_block", block + 1)
        self.set("created_at", time.time())
        self.db.execute("INSERT INTO streams VALUES ('protocols',?,?,?,'protocols',0)", (block, block_hash, block + 1))
        self.db.commit()

    def add_entity(self, addr: str, kind: str, block: int) -> None:
        if (
            addr in CORE
            or addr in CONTRACTS.values()
            or self.db.execute("SELECT 1 FROM entities WHERE address=?", (addr,)).fetchone()
        ):
            return
        # A new address must never be added behind a stream's committed cursor.
        row = self.db.execute(
            """SELECT s.name FROM streams s LEFT JOIN entities e ON e.stream=s.name
            WHERE s.kind='entities' AND s.cursor<? GROUP BY s.name HAVING count(e.address)<100
            ORDER BY s.start_block DESC LIMIT 1""",
            (block,),
        ).fetchone()
        if row:
            stream = str(row[0])
        else:
            stream = f"entities-{block}-{addr[2:10]}"
            self.db.execute("INSERT INTO streams VALUES (?,?,NULL,?,'entities',0)", (stream, block - 1, block))
        self.db.execute("INSERT INTO entities VALUES (?,?,?,?)", (addr, kind, stream, block))

    def token(self, addr: str, block: int) -> None:
        if addr in CORE:
            return
        self.db.execute("INSERT OR IGNORE INTO tokens(address,first_block) VALUES (?,?)", (addr, block))
        self.add_entity(addr, "token", block)

    def facts(self, table: str, addr: str, fields: dict[str, Any]) -> None:
        if table not in {"tokens", "pools"}:
            raise ValueError("invalid table")
        row = self.db.execute(f"SELECT facts FROM {table} WHERE address=?", (addr,)).fetchone()  # noqa: S608
        if row:
            facts = json.loads(row[0])
            facts.update(fields)
            self.db.execute(f"UPDATE {table} SET facts=? WHERE address=?", (encoded(facts).decode(), addr))  # noqa: S608

    def transfer(self, token: str, fields: dict[str, Any]) -> None:
        if fields["from"] == fields["to"]:
            return
        value = int(fields["value"])
        for wallet, delta in ((fields["from"], -value), (fields["to"], value)):
            if wallet == ZERO:
                continue
            row = self.db.execute("SELECT value FROM balances WHERE token=? AND wallet=?", (token, wallet)).fetchone()
            balance = (int(row[0]) if row else 0) + delta
            self.db.execute("INSERT OR REPLACE INTO balances VALUES (?,?,?)", (token, wallet, str(balance)))
            if balance < 0:
                self.quality("negative_event_balance")

    def apply(self, log: dict[str, Any], source: str, stream: str = "direct") -> None:
        block = int(log["blockNumber"], 16)
        ts = int(log["blockTimestamp"], 16) if log.get("blockTimestamp") else None
        key = log["blockHash"] + ":" + log["logIndex"]
        if self.db.execute("SELECT 1 FROM seen WHERE id=?", (key,)).fetchone():
            return
        try:
            fields = decode(log, source)
        except (ValueError, KeyError, TypeError, IndexError, OverflowError):
            fields = {"event": "DECODE_ERROR", "source": source}
            self.quality("decode_error")
        event = fields["event"]
        token = fields.get("token", fields.get("base"))
        self.db.execute("INSERT INTO seen VALUES (?,?,?,?,?,?,?)", (key, block, event, source, token, ts, stream))
        self.db.execute(
            "INSERT INTO event_counts VALUES (?,1) ON CONFLICT(event) DO UPDATE SET count=count+1", (event,)
        )
        if event in {"TokenCreate", "TokenCreated", "FlapDirectLaunched"}:
            token = address(fields["token"])
            self.token(token, block)
            self.db.execute(
                """UPDATE tokens SET platform=?,birth_block=?,birth_ts=?,creator=? WHERE address=?""",
                (source, block, ts, fields["creator"], token),
            )
            self.facts("tokens", token, {"creation": fields})
        elif event in {"PairCreated", "PoolCreated"}:
            pool = address(fields["pair"] if event == "PairCreated" else fields["pool"])
            kind = "pool_v2" if event == "PairCreated" else "pool_v3"
            for token_addr in (fields["token0"], fields["token1"]):
                self.token(token_addr, block)
            self.db.execute(
                "INSERT OR IGNORE INTO pools VALUES (?,?,?,?,?,'{}')",
                (pool, kind, fields["token0"], fields["token1"], block),
            )
            self.add_entity(pool, kind, block)
        elif event == "Transfer" and source == "token":
            self.transfer(log["address"].lower(), fields)
        elif event in {"Sync", "Swap"} and source in {"pool_v2", "pool_v3"}:
            self.facts("pools", log["address"].lower(), {event: fields, "block": block, "ts": ts})
        elif token and source in {"four_v2", "flap_curve", "flap_direct", "flap_vault"}:
            self.token(token, block)
            self.facts("tokens", token, {event: fields, "last_protocol_block": block})

    def members(self, stream: str) -> dict[str, str]:
        if stream == "protocols":
            return {v: k for k, v in CONTRACTS.items()}
        return {r["address"]: r["kind"] for r in self.db.execute("SELECT * FROM entities WHERE stream=?", (stream,))}

    def commit_range(self, stream: str, end: int, end_hash: str, logs: list[dict[str, Any]]) -> None:
        members = self.members(stream)
        with self.db:
            cursor = self.db.execute("SELECT cursor FROM streams WHERE name=?", (stream,)).fetchone()[0]
            if end <= cursor or any(not cursor < int(log["blockNumber"], 16) <= end for log in logs):
                raise ValueError("nonmonotone committed range")
            # Raw archives retain all history. Only the current range needs duplicate keys in state.
            # Stream ownership of addresses is immutable; ranges never overlap or rewind.
            self.db.execute("DELETE FROM seen WHERE stream=?", (stream,))
            for log in sorted(logs, key=lambda x: (int(x["blockNumber"], 16), int(x["logIndex"], 16))):
                self.apply(log, members[log["address"].lower()], stream)
            self.db.execute(
                "UPDATE streams SET cursor=?,hash=?,updated=? WHERE name=?", (end, end_hash, time.time(), stream)
            )

    def snapshot(self, target: Path) -> None:
        self.db.commit()
        with sqlite3.connect(target) as dest:
            self.db.backup(dest)
            if dest.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ValueError("SQLite integrity check failed")

    def status(self) -> dict[str, Any]:
        protocol = self.db.execute("SELECT cursor,updated FROM streams WHERE name='protocols'").fetchone()
        row = self.db.execute("SELECT min(cursor),count(*) FROM streams WHERE kind='entities'").fetchone()
        by_platform = {
            str(r[0]): r[1] for r in self.db.execute("SELECT platform,count(*) FROM tokens GROUP BY platform")
        }
        events = {r[0]: r[1] for r in self.db.execute("SELECT event,count FROM event_counts")}
        coverage = {
            r[0].removeprefix("covered_time:"): json.loads(r[1])
            for r in self.db.execute("SELECT key,value FROM meta WHERE key LIKE 'covered_time:%'")
        }
        complete_24h = 0
        for token in self.db.execute("""SELECT t.birth_ts,t.birth_block,e.start_block,e.stream FROM tokens t
            JOIN entities e ON e.address=t.address WHERE t.birth_ts IS NOT NULL"""):
            if token[2] <= token[1] and coverage.get(token[3], 0) >= token[0] + 86400:
                complete_24h += 1
        return {
            "schema": 1,
            "updated_at": time.time(),
            "mode": "RESEARCH_ONLY",
            "real_trades": 0,
            "realized_pnl_usdt": "0",
            "strategy_validated": False,
            "last_session": self.get("last_session"),
            "last_cycle": self.get("last_cycle"),
            "last_finalized_head": self.get("last_finalized_head"),
            "initial_block": self.get("initial_block"),
            "collection_started_at": self.get("created_at"),
            "births_observed": self.db.execute("SELECT count(*) FROM tokens WHERE birth_block IS NOT NULL").fetchone()[
                0
            ],
            "births_with_24h_token_logs": complete_24h,
            "protocol_covered_until": coverage.get("protocols"),
            "protocol_cursor": protocol[0] if protocol else None,
            "protocol_last_success": protocol[1] if protocol else None,
            "oldest_entity_cursor": row[0],
            "entity_streams": row[1],
            "tokens_by_platform": by_platform,
            "tokens": sum(by_platform.values()),
            "events": events,
            "quality": {r[0]: r[1] for r in self.db.execute("SELECT * FROM quality")},
            "coverage_claim": "Only named contracts and observed entities since initial_block; no full-life claim",
            "holder_basis": "Transfer-event reconstruction, not verified balances; negative balances are flagged",
            "availability": "chain time and received time are distinct; backfill cannot be used as live knowledge",
        }
