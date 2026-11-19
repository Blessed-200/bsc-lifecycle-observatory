"""Historical PancakeSwap V2 PairCreated backfill from verified public BSC RPC.

Reads only. Captures every factory event in exact bounded windows, including
pools with no trades or failed launches. Errors leave incomplete windows, never
silent zero counts. No wallet, returns, forecast or financial orders.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

RPC = "https://bsc-rpc.publicnode.com"
FACTORY = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73"
TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
CORE = {
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
    "0x55d398326f99059ff775485246999027b3197955",
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",
}
WINDOWS = [(132000000, 132005000), (133000000, 133005000), (134000000, 134005000)]
SIZE = 100
RATE_SECONDS = 0.55


class ProviderFailure(Exception):
    pass


_last = 0.0


def rpc(method: str, params: list[Any]) -> Any:
    global _last
    payload = json.dumps({"jsonrpc": "2.0", "id": 77, "method": method, "params": params}).encode()
    req = urllib.request.Request(RPC, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    for retry in range(4):
        delta = RATE_SECONDS - (time.monotonic() - _last)
        if delta > 0:
            time.sleep(delta)
        _last = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=25) as response:
                raw = response.read(4_000_001)
            if len(raw) >= 4_000_001:
                raise ProviderFailure("oversized response")
            doc = json.loads(raw)
            if not isinstance(doc, dict) or doc.get("id") != 77 or doc.get("result") is None:
                raise ProviderFailure("RPC result absent or malformed: "+str(doc.get("error"))[:200])
            return doc["result"]
        except (urllib.error.URLError, TimeoutError, ProviderFailure) as exc:
            if retry == 3:
                raise ProviderFailure(str(exc)[:250]) from exc
            time.sleep(2 ** (retry + 1))
    raise AssertionError("unreachable")


def get_header(block: int) -> dict[str, Any]:
    r = rpc("eth_getBlockByNumber", [hex(block), False])
    if not isinstance(r, dict) or int(r.get("number", "0x0"), 16) != block or not r.get("hash"):
        raise ProviderFailure("block header identity mismatch")
    return r


def decode_event(log: dict[str, Any]) -> dict[str, Any]:
    topics = log.get("topics")
    if not isinstance(topics, list) or len(topics) != 3 or str(topics[0]).lower() != TOPIC:
        raise ProviderFailure("topic mismatch")
    if str(log.get("address", "")).lower() != FACTORY or log.get("removed") is True:
        raise ProviderFailure("factory mismatch or removed log")
    data = str(log.get("data", ""))
    if not data.startswith("0x") or len(data) < 130:
        raise ProviderFailure("unexpected PairCreated payload")
    t0 = "0x" + str(topics[1])[-40:].lower()
    t1 = "0x" + str(topics[2])[-40:].lower()
    pair = "0x" + data[26:66].lower()
    if any(len(x) != 42 or not x.startswith("0x") for x in (t0,t1,pair)):
        raise ProviderFailure("invalid decoded address")
    quote = [x for x in (t0,t1) if x in CORE]
    noncore = [x for x in (t0,t1) if x not in CORE]
    return {
        "chain_id": 56, "block": int(log["blockNumber"], 16),
        "block_hash": log["blockHash"], "log_index": int(log["logIndex"], 16),
        "tx_hash": log["transactionHash"], "factory": FACTORY,
        "pool": pair, "token0": t0, "token1": t1,
        "quote_core": quote[0] if len(quote) == 1 and len(noncore) == 1 else "",
        "memecoin_candidate": noncore[0] if len(quote) == 1 and len(noncore) == 1 else "",
        "pair_index": int(data[66:130],16),
    }


def main() -> None:
    root = Path("evidence/factory-backfill")
    root.mkdir(parents=True, exist_ok=True)
    journal_path = root / "coverage.json"
    ledger: list[dict[str, Any]] = []
    events: dict[str, dict[str, Any]] = {}
    latest_state = {"phase":"RUNNING", "coverage": ledger,"windows":WINDOWS, "financial_execution":False}
    journal_path.write_text(json.dumps(latest_state, indent=2)+"\n")
    if rpc("eth_chainId", []) != "0x38":
        raise ProviderFailure("wrong chain")
    headers = []
    for s,e in WINDOWS:
        left=get_header(s)
        right=get_header(e)
        headers.append({"start":s,"start_hash":left["hash"],"start_ts":int(left["timestamp"],16),
                        "end":e,"end_hash":right["hash"],"end_ts":int(right["timestamp"],16)})
    errors: list[dict[str, Any]] = []
    for a,b in WINDOWS:
        for start in range(a,b+1,SIZE):
            end=min(start+SIZE-1,b)
            rec: dict[str, Any]={"start":start,"end":end,"status":"FAILED"}
            try:
                logs=rpc("eth_getLogs",[{"fromBlock":hex(start),"toBlock":hex(end),
                       "address":FACTORY,"topics":[TOPIC]}])
                if not isinstance(logs,list) or len(logs)>=5000:
                    raise ProviderFailure("capped/invalid log list")
                for log in logs:
                    obj=decode_event(log)
                    if not start<=obj["block"]<=end:
                        raise ProviderFailure("log outside interval")
                    key=obj["block_hash"]+":"+str(obj["log_index"])
                    if key in events and events[key]!=obj:
                        raise ProviderFailure("conflicting duplicated identity")
                    events[key]=obj
                rec.update({"status":"COMPLETE","count":len(logs),
                            "sha256":hashlib.sha256(json.dumps(logs,sort_keys=True).encode()).hexdigest()})
            except (ProviderFailure,KeyError,ValueError) as exc:
                rec["error"]=type(exc).__name__+":"+str(exc)[:230]
                errors.append(rec)
            ledger.append(rec)
            latest_state["coverage"]=ledger
            latest_state["events_decoded"]=len(events)
            journal_path.write_text(json.dumps(latest_state,sort_keys=True,indent=2)+"\n")
            if len(ledger)%20==0:
                print("progress chunks",len(ledger),"events",len(events),"failed",len(errors),flush=True)
    with gzip.open(root/"pair_created.jsonl.gz","wt",encoding="utf-8") as out:
        for row in sorted(events.values(),key=lambda x:(x["block"],x["log_index"])):
            out.write(json.dumps(row,sort_keys=True)+"\n")
    complete_ranges=sum(x["status"]=="COMPLETE" for x in ledger)
    report={
       "schema":"pancake_v2_factory_backfill_v1",
       "rpc":RPC,"chain_id":56, "windows":headers,
       "chunks_total":len(ledger),"chunks_complete":complete_ranges,
       "chunks_failed":len(errors), "pairs_found":len(events),
       "quoted_memecoin_pairs":sum(bool(x["memecoin_candidate"]) for x in events.values()),
       "pool_universe_complete_for_windows":len(errors)==0,
       "state":"DATA_READY_FOR_FURTHER_REPLAY" if len(errors)==0 else "PARTIAL_NOT_COMPLETE",
       "not_census_of_other_protocols":True,
       "prices_reconstructed":False,"executed_returns":None,"capital_enabled":False,
       "reason":"PairCreated is a birth census, not a price history or trading signal",
    }
    (root/"summary.json").write_text(json.dumps(report,sort_keys=True,indent=2)+"\n")
    print(json.dumps(report,indent=2),flush=True)


if __name__ == "__main__":
    main()
