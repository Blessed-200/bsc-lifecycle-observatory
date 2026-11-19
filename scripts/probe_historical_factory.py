"""Bounded historical BSC factory event availability probe (read only).

This is a transport diagnostic for block-verified 2026 older ranges, not
a historical trading strategy. Never infer zero births from RPC errors.
"""

from __future__ import annotations

import hashlib
import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

RPCS = (
    "https://bsc-rpc.publicnode.com",
    "https://bsc-dataseed.bnbchain.org",
    "https://bsc-dataseed1.defibit.io",
)
FACTORY = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73"
PAIR_CREATED = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
BLOCK_RANGES = [(133000000,133000100),(132000000,132000100),(134000000,134000100)]


def request(url: str, method: str, params: list[Any]) -> Any:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=18) as response:
        raw = response.read(4_000_001)
        if len(raw) > 4_000_000:
            raise ValueError("oversized RPC response")
        doc = json.loads(raw)
    if not isinstance(doc, dict) or doc.get("id") != 1:
        raise ValueError("unexpected RPC envelope")
    if "error" in doc:
        raise ValueError("RPC_ERROR: " + str(doc["error"])[:200])
    if "result" not in doc or doc["result"] is None:
        raise ValueError("RPC_RESULT_MISSING")
    return doc["result"]


def main() -> None:
    out = Path("evidence/factory-probe")
    out.mkdir(parents=True, exist_ok=True)
    results = []
    verified = 0
    for endpoint in RPCS:
        item: dict[str, Any] = {"host": endpoint.split("//")[1], "ranges": []}
        try:
            chain = request(endpoint, "eth_chainId", [])
            if chain != "0x38":
                raise ValueError("WRONG_CHAIN")
            item["chain_id_verified"] = 56
        except (ValueError, urllib.error.URLError, TimeoutError) as exc:
            item["chain_id_verified"] = False
            item["error"] = type(exc).__name__ + ":" + str(exc)[:180]
            results.append(item)
            continue
        for start, end in BLOCK_RANGES:
            cell: dict[str, Any] = {"from_block": start, "to_block": end, "status": "UNKNOWN"}
            try:
                lo = request(endpoint, "eth_getBlockByNumber", [hex(start), False])
                hi = request(endpoint, "eth_getBlockByNumber", [hex(end), False])
                if int(lo["number"], 16) != start or int(hi["number"], 16) != end:
                    raise ValueError("HEADER_MISMATCH")
                payload = {"fromBlock": hex(start), "toBlock": hex(end),
                           "address": FACTORY, "topics": [PAIR_CREATED]}
                logs = request(endpoint, "eth_getLogs", [payload])
                if not isinstance(logs, list):
                    raise ValueError("NOT_LOG_LIST")
                for log in logs:
                    block = int(log["blockNumber"], 16)
                    if not start <= block <= end or log["address"].lower() != FACTORY:
                        raise ValueError("LOG_RANGE_OR_IDENTITY_MISMATCH")
                    if log["topics"][0].lower() != PAIR_CREATED:
                        raise ValueError("TOPIC_MISMATCH")
                cell["status"] = "OK_VERIFIED_HEADERS_AND_LOGS"
                cell["start_header_hash"] = lo["hash"]
                cell["end_header_hash"] = hi["hash"]
                cell["pair_created_count"] = len(logs)
                cell["logs_sha256"] = hashlib.sha256(json.dumps(logs,sort_keys=True).encode()).hexdigest()
                verified += 1
            except (ValueError, urllib.error.URLError, TimeoutError, KeyError, TypeError) as exc:
                cell["status"] = "ERROR_NOT_ZERO"
                cell["error"] = type(exc).__name__ + ":" + str(exc)[:250]
            item["ranges"].append(cell)
        results.append(item)
    report = {
        "schema": 1, "purpose": "historical_bsc_rpc_capability_not_trading",
        "verified_range_provider_pairs": verified,
        "results": results,
        "no_data_claim_when_error": True,
        "strategy_validated": False, "capital_enabled": False,
    }
    (out/"report.json").write_text(json.dumps(report, indent=2, sort_keys=True)+"\n")
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    main()
