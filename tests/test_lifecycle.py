from __future__ import annotations

import asyncio
import base64
import io
import sqlite3
import tarfile
from pathlib import Path
from typing import Any, cast

import pytest

from bsc_lifecycle.archive import Github, TransportError, inspect_archive, publish
from bsc_lifecycle.codec import SPEC, ZERO, decode
from bsc_lifecycle.collector import Collector, IntegrityError, validate_logs
from bsc_lifecycle.network import Oversize, Reader
from bsc_lifecycle.store import Journal, Store, encoded, sha

TOKEN = "0x" + "11" * 20
OTHER = "0x" + "22" * 20
CREATOR = "0x" + "33" * 20
BLOCK_HASH = "0x" + "44" * 32


def uint(value: int) -> bytes:
    return value.to_bytes(32)


def addr(value: str) -> bytes:
    return bytes.fromhex(value[2:]).rjust(32, b"\0")


def event(name: str, source: str) -> dict[str, Any]:
    return next(e for e in SPEC["events"] if e["name"] == name and source in e["sources"])


def log(name: str = "Transfer", source: str = "token", index: int = 1, block: int = 101) -> dict[str, Any]:
    spec = event(name, source)
    value: dict[str, Any] = {
        "address": TOKEN,
        "topics": [spec["topic"]],
        "data": "0x",
        "blockHash": BLOCK_HASH,
        "blockNumber": hex(block),
        "blockTimestamp": "0x64",
        "transactionHash": "0x" + "55" * 32,
        "transactionIndex": "0x0",
        "logIndex": hex(index),
        "removed": False,
    }
    if name == "Transfer":
        value["topics"] += ["0x" + addr(ZERO).hex(), "0x" + addr(CREATOR).hex()]
        value["data"] = "0x" + uint(1000).hex()
    return value


def test_real_documented_flap_log_decodes_without_price_unit_guess() -> None:
    value = {
        "topics": ["0x115c78ad17c4763fb97bca94f3e59dc8cb2e59c9d3862f24a694ec401200f562"],
        "data": "0x000000000000000000000000f99a7341105b7cef7cb8dbe781b08a5817fb777700000000000000000000000000000000000000000037ea97597c52c8e58cbfc9",
    }
    decoded = decode(value, "flap_curve")
    assert decoded["event"] == "FlapTokenCirculatingSupplyChanged"
    assert decoded["token"] == "0xf99a7341105b7cef7cb8dbe781b08a5817fb7777"
    assert isinstance(decoded["newSupply"], str)
    assert int(decoded["newSupply"]) > 2**53
    assert decode(value, "four_v2")["event"] == "UNKNOWN"


def test_creation_name_cannot_overwrite_event_type() -> None:
    spec = event("TokenCreated", "flap_curve")
    head = uint(100) + addr(CREATOR) + uint(1) + addr(TOKEN)
    tail = b""
    for text in ("Transfer", "FAKE", "cid"):
        head += uint(7 * 32 + len(tail))
        tail += uint(len(text)) + text.encode().ljust(32, b"\0")
    result = decode({"topics": [spec["topic"]], "data": "0x" + (head + tail).hex()}, "flap_curve")
    assert result["event"] == "TokenCreated"
    assert result["name"] == "Transfer"


@pytest.mark.parametrize("mutation", ["removed", "address", "range", "hash", "topic", "data"])
def test_bad_log_envelopes_rejected(mutation: str) -> None:
    value = log()
    changes: dict[str, Any] = {
        "removed": True,
        "address": OTHER,
        "range": "0xff",
        "hash": "0x1",
        "topic": ["bad"],
        "data": "0xzz",
    }
    field = {"range": "blockNumber", "hash": "blockHash", "topic": "topics"}.get(mutation, mutation)
    value[field] = changes[mutation]
    with pytest.raises(IntegrityError):
        validate_logs([value], {TOKEN: "token"}, 100, 110)


def test_conflicting_duplicate_rejected_and_exact_duplicate_deduplicated() -> None:
    value = log()
    assert len(validate_logs([value, value], {TOKEN: "token"}, 100, 110)) == 1
    conflict = {**value, "data": "0x" + uint(999).hex()}
    with pytest.raises(IntegrityError, match="conflicting"):
        validate_logs([value, conflict], {TOKEN: "token"}, 100, 110)


def test_result_cap_is_not_a_complete_range() -> None:
    with pytest.raises(Oversize):
        validate_logs([log()] * 5000, {TOKEN: "token"}, 100, 110)


def test_transfer_balances_deduplicate_and_do_not_claim_verified_holders(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite")
    store.token(TOKEN, 101)
    value = log()
    store.apply(value, "token")
    store.apply(value, "token")
    transfer = log(index=2)
    transfer["topics"][1:] = ["0x" + addr(CREATOR).hex(), "0x" + addr(OTHER).hex()]
    transfer["data"] = "0x" + uint(400).hex()
    store.apply(transfer, "token")
    assert dict(store.db.execute("SELECT wallet,value FROM balances")) == {CREATOR: "600", OTHER: "400"}
    assert store.status()["events"]["Transfer"] == 2
    assert "not verified" in store.status()["holder_basis"]
    store.db.close()


def test_new_entity_behind_existing_cursor_gets_own_stream(tmp_path: Path) -> None:
    store = Store(tmp_path / "s.sqlite")
    store.token(TOKEN, 101)
    original = store.db.execute("SELECT stream FROM entities WHERE address=?", (TOKEN,)).fetchone()[0]
    store.db.execute("UPDATE streams SET cursor=200 WHERE name=?", (original,))
    store.token(OTHER, 150)
    second = store.db.execute("SELECT stream FROM entities WHERE address=?", (OTHER,)).fetchone()[0]
    assert second != original
    assert store.db.execute("SELECT cursor FROM streams WHERE name=?", (second,)).fetchone()[0] == 149
    store.db.close()


def test_state_compacts_duplicate_keys_without_losing_lifetime_counts(tmp_path: Path) -> None:
    store = Store(tmp_path / "s.sqlite")
    store.token(TOKEN, 101)
    stream = store.db.execute("SELECT stream FROM entities").fetchone()[0]
    store.commit_range(stream, 101, BLOCK_HASH, [log(block=101)])
    second = log(index=2, block=102)
    store.commit_range(stream, 102, BLOCK_HASH, [second])
    assert store.status()["events"]["Transfer"] == 2
    assert store.db.execute("SELECT count(*) FROM seen").fetchone()[0] == 1
    with pytest.raises(ValueError, match="nonmonotone"):
        store.commit_range(stream, 103, BLOCK_HASH, [second])
    assert store.status()["events"]["Transfer"] == 2
    assert store.db.execute("SELECT cursor FROM streams").fetchone()[0] == 102
    store.db.close()


def test_negative_reconstructed_balance_flags_quality(tmp_path: Path) -> None:
    store = Store(tmp_path / "s.sqlite")
    value = log()
    value["topics"][1] = "0x" + addr(OTHER).hex()
    store.apply(value, "token")
    assert store.status()["quality"]["negative_event_balance"] == 1
    store.db.close()


class Chain:
    def __init__(self, rows: list[dict[str, Any]], *, reorg: bool = False, oversize: bool = False) -> None:
        self.rows, self.reorg, self.oversize = rows, reorg, oversize
        self.requests: list[dict[str, Any]] = []
        self.headers = 0

    async def block(self, number: int) -> dict[str, Any]:
        self.headers += 1
        block_hash = "0x" + "66" * 32 if self.reorg and self.headers > 2 else BLOCK_HASH
        return {"number": hex(number), "hash": block_hash, "timestamp": "0x64"}

    async def rpc(self, _method: str, params: list[Any]) -> list[dict[str, Any]]:
        query = params[0]
        self.requests.append(query)
        if self.oversize and int(query["toBlock"], 16) - int(query["fromBlock"], 16) > 2:
            raise Oversize()
        return [r for r in self.rows if r["address"] in query["address"]]


def test_reorg_during_read_preserves_cursor(tmp_path: Path) -> None:
    store = Store(tmp_path / "s.sqlite")
    store.initialize(100, BLOCK_HASH)
    journal = Journal(tmp_path)
    chain = Chain([], reorg=True)
    with pytest.raises(IntegrityError):
        asyncio.run(Collector(store, journal, cast(Reader, chain)).scan("protocols", 110))
    assert store.status()["protocol_cursor"] == 100
    journal.file.close()
    store.db.close()


def test_oversized_range_is_split_before_advancing(tmp_path: Path) -> None:
    store = Store(tmp_path / "s.sqlite")
    store.initialize(100, BLOCK_HASH)
    journal = Journal(tmp_path)
    chain = Chain([], oversize=True)
    assert asyncio.run(Collector(store, journal, cast(Reader, chain)).scan("protocols", 110))
    assert store.status()["protocol_cursor"] == 103
    assert len(chain.requests) == 3
    journal.file.close()
    store.db.close()


def test_entity_requests_respect_ten_address_limit(tmp_path: Path) -> None:
    store = Store(tmp_path / "s.sqlite")
    for i in range(1, 26):
        store.token("0x" + f"{i:040x}", 101)
    stream = store.db.execute("SELECT name FROM streams").fetchone()[0]
    journal = Journal(tmp_path)
    chain = Chain([])
    asyncio.run(Collector(store, journal, cast(Reader, chain)).scan(stream, 110))
    assert [len(q["address"]) for q in chain.requests] == [10, 10, 5]
    journal.file.close()
    store.db.close()


def test_rpc_mutation_never_reaches_network(tmp_path: Path) -> None:
    reader = Reader(cast(Any, None), Journal(tmp_path))
    with pytest.raises(ValueError, match="forbidden"):
        asyncio.run(reader.rpc("eth_sendRawTransaction", ["0x"]))
    reader.journal.file.close()


class ArchiveSink:
    def __init__(self, path: Path, fail: bool = False) -> None:
        self.parent = None
        self.path, self.fail = path, fail
        self.pointer: dict[str, Any] = {}

    def publish(self, source: Path, pointer: dict[str, Any], _status: dict[str, Any]) -> None:
        if self.fail:
            raise TransportError("CAS conflict")
        self.path.write_bytes(source.read_bytes())
        self.pointer = pointer


def make_archive(tmp_path: Path, fail: bool = False) -> tuple[Store, Journal, ArchiveSink]:
    store = Store(tmp_path / "s.sqlite")
    store.initialize(100, BLOCK_HASH)
    journal = Journal(tmp_path)
    journal.write("http", {"body_b64": base64.b64encode(b'{"result": []}').decode()})
    sink = ArchiveSink(tmp_path / "batch.tar.gz", fail)
    return store, journal, sink


def test_checkpoint_restores_exact_cursor_and_original_bytes(tmp_path: Path) -> None:
    store, journal, sink = make_archive(tmp_path)
    publish(store, journal, tmp_path, cast(Github, sink), {"test": True})
    assert sha(sink.path.read_bytes()) == sink.pointer["sha256"]
    target = tmp_path / "restored.sqlite"
    manifest = inspect_archive(sink.path, target)
    assert any(n.startswith("raw-") for n in manifest["files"])
    with sqlite3.connect(target) as db:
        assert db.execute("SELECT cursor FROM streams WHERE name='protocols'").fetchone()[0] == 100
    assert not [p for p in tmp_path.glob("raw-*.jsonl") if p != journal.path]
    with pytest.raises(TransportError, match="overwrite"):
        inspect_archive(sink.path, target)
    journal.file.close()
    store.db.close()


def test_failed_publication_preserves_unacknowledged_raw_files(tmp_path: Path) -> None:
    store, journal, sink = make_archive(tmp_path, fail=True)
    with pytest.raises(TransportError, match="CAS"):
        publish(store, journal, tmp_path, cast(Github, sink), {})
    assert any(p.stat().st_size for p in tmp_path.glob("raw-*.jsonl"))
    journal.file.close()
    store.db.close()


@pytest.mark.parametrize("attack", ["tamper", "path", "duplicate", "missing"])
def test_archive_corruption_and_unsafe_members_rejected(tmp_path: Path, attack: str) -> None:
    store, journal, sink = make_archive(tmp_path)
    publish(store, journal, tmp_path, cast(Github, sink), {})
    bad = tmp_path / "bad.tar.gz"
    with tarfile.open(sink.path) as source, tarfile.open(bad, "w:gz") as dest:
        for member in source.getmembers():
            if attack == "missing" and member.name == "state.sqlite":
                continue
            file = source.extractfile(member)
            assert file is not None
            data = file.read()
            if member.name == "status.json":
                if attack == "tamper":
                    data = encoded({"lie": True})
                if attack == "path":
                    member.name = "../status.json"
                if attack == "duplicate":
                    dest.addfile(member, io.BytesIO(data))
            member.size = len(data)
            dest.addfile(member, io.BytesIO(data))
    with pytest.raises(TransportError):
        inspect_archive(bad)
    journal.file.close()
    store.db.close()


def test_raw_file_fsync_failure_cannot_advance_cursor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = Store(tmp_path / "s.sqlite")
    store.initialize(100, BLOCK_HASH)
    journal = Journal(tmp_path)

    def fail() -> None:
        raise OSError("disk full")

    monkeypatch.setattr(journal, "flush", fail)
    with pytest.raises(OSError, match="disk full"):
        asyncio.run(Collector(store, journal, cast(Reader, Chain([]))).scan("protocols", 110))
    assert store.status()["protocol_cursor"] == 100
    journal.file.close()
    store.db.close()
