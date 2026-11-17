"""Decode explicitly documented event layouts; retain unknown logs without guessing."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

ZERO = "0x" + "0" * 40
ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}\Z")
HASH = re.compile(r"0x[0-9a-fA-F]{64}\Z")
SPEC: dict[str, Any] = json.loads(Path(__file__).with_name("contracts.json").read_text())
CONTRACTS: dict[str, str] = SPEC["contracts"]
EVENTS: dict[str, dict[str, Any]] = {e["topic"]: e for e in SPEC["events"]}


def address(value: str) -> str:
    if not isinstance(value, str) or not ADDRESS.fullmatch(value):
        raise ValueError("invalid address")
    return value.lower()


def word_value(word: bytes, kind: str) -> str | int:
    if len(word) != 32:
        raise ValueError("short ABI word")
    if kind == "address":
        if any(word[:12]):
            raise ValueError("noncanonical ABI address")
        return "0x" + word[12:].hex()
    if kind == "bytes32":
        return "0x" + word.hex()
    signed = kind.startswith("int")
    bits = int(kind[3:] if signed else kind[4:])
    value = int.from_bytes(word, signed=signed)
    lower, upper = (-(2 ** (bits - 1)), 2 ** (bits - 1)) if signed else (0, 2**bits)
    if not lower <= value < upper:
        raise ValueError("noncanonical ABI integer")
    # JSON strings preserve uint256 precision in browsers and downstream readers.
    return str(value)


def decode(log: dict[str, Any], source: str) -> dict[str, Any]:
    topics = log["topics"]
    spec = EVENTS.get(topics[0]) if topics else None
    if spec is None or source not in spec["sources"]:
        return {"event": "UNKNOWN", "source": source}
    data = bytes.fromhex(log["data"][2:])
    fields = spec["fields"]
    head_size = sum(not f[2] for f in fields) * 32
    if len(data) < head_size or len(data) % 32:
        raise ValueError("malformed ABI head")
    if len(topics) != 1 + sum(f[2] for f in fields):
        raise ValueError("unexpected indexed fields")
    result: dict[str, Any] = {"event": spec["name"], "source": source}
    position, topic_position = 0, 1
    for name, kind, indexed in fields:
        if indexed:
            word = bytes.fromhex(topics[topic_position][2:])
            topic_position += 1
        else:
            word = data[position : position + 32]
            position += 32
        if kind in {"string", "bytes"}:
            offset = int.from_bytes(word)
            if indexed or offset < head_size or offset % 32 or offset + 32 > len(data):
                raise ValueError("invalid ABI dynamic offset")
            length = int.from_bytes(data[offset : offset + 32])
            if offset + 32 + length > len(data):
                raise ValueError("short ABI dynamic data")
            value = data[offset + 32 : offset + 32 + length]
            result[name] = value.decode("utf-8", errors="replace") if kind == "string" else "0x" + value.hex()
        else:
            result[name] = word_value(word, kind)
    return result
