"""Onchain V2 Sync+Swap historical follow-up for a deterministic birth cohort.

Pure blockchain data, no funds. Deterministic subset of 106 historical births
from a verified, fixed GitHub Actions artifact. Fail closed on missing logs,
RPC caps, invalid identities or non-contiguous range coverage.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

RPC_URL = "https://bsc-rpc.publicnode.com"
CHAIN = 56
SYNC = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
SWAP = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
EXPECTED_BIRTH_FILE_SHA256 = "d2417f8f81d725fb8171b0567e9c0056c3d776c3bdf6715f140953e3c4fd9575"
BLOCK_START = 134000000
BLOCK_END = 134014000
STEP = 100
MAX_POOLS = 24
REQUEST_DELAY = 0.53
_last = 0.0


class EvidenceError(Exception):
    pass


def rpc(method: str, params: list[Any]) -> Any:
    global _last
    body = json.dumps({"jsonrpc":"2.0","id":12,"method":method,"params":params}).encode()
    req = urllib.request.Request(RPC_URL, data=body,
             headers={"Content-Type":"application/json"}, method="POST")
    for try_no in range(4):
        sleep = REQUEST_DELAY - (time.monotonic() - _last)
        if sleep > 0:
            time.sleep(sleep)
        _last = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=25) as response:
                raw = response.read(4_000_001)
            if len(raw) > 4_000_000:
                raise EvidenceError("RPC_TOO_LARGE")
            value = json.loads(raw)
            if not isinstance(value, dict) or value.get("id") != 12:
                raise EvidenceError("RPC_WRONG_ENVELOPE")
            if value.get("result") is None:
                raise EvidenceError("RPC_NO_RESULT:" + str(value.get("error"))[:100])
            return value["result"]
        except (TimeoutError, ValueError, urllib.error.URLError, EvidenceError) as exc:
            if try_no == 3:
                raise EvidenceError(type(exc).__name__+":"+str(exc)[:160]) from exc
            time.sleep(2**try_no)
    raise AssertionError("unreachable")


def header(block: int, cache: dict[int,int]) -> int:
    if block not in cache:
        h = rpc("eth_getBlockByNumber", [hex(block), False])
        if int(h.get("number","0x0"),16) != block:
            raise EvidenceError("BLOCK_HEADER_MISMATCH")
        cache[block] = int(h["timestamp"],16)
    return cache[block]


def latest_boundary(start: int, stop: int, min_ts: int, cache: dict[int,int]) -> int:
    """Smallest historical block whose exact block timestamp >= min_ts."""
    a,b=start,stop
    while a<b:
        middle=(a+b)//2
        if header(middle,cache) < min_ts:
            a=middle+1
        else:
            b=middle
    return a


def event(log: dict[str, Any], pairs: dict[str,dict[str,Any]]) -> dict[str, Any] | None:
    addr=str(log.get("address","")).lower()
    topics=log.get("topics") or []
    if addr not in pairs or len(topics)==0 or log.get("removed") is True:
        raise EvidenceError("LOG_ADDRESS_OR_REMOVED")
    topic=str(topics[0]).lower()
    if topic not in (SYNC,SWAP):
        raise EvidenceError("UNEXPECTED_LOG_TOPIC")
    block=int(log["blockNumber"],16)
    if not BLOCK_START<=block<=BLOCK_END:
        raise EvidenceError("LOG_OUT_OF_SCOPE")
    data=str(log.get("data",""))
    if not data.startswith("0x"):
        raise EvidenceError("INVALID_LOG_HEX")
    base={"pool":addr,"block":block,"log_index":int(log["logIndex"],16),
          "tx_hash":log.get("transactionHash"),"block_hash":log.get("blockHash"),
          "kind":"SYNC" if topic==SYNC else "SWAP"}
    if topic==SYNC:
        if len(data)!=130:
            raise EvidenceError("SYNC_SHAPE_WRONG")
        r0=int(data[2:66],16)
        r1=int(data[66:130],16)
        return {**base,"reserve0":str(r0),"reserve1":str(r1)}
    if len(data)!=258:
        raise EvidenceError("SWAP_SHAPE_WRONG")
    nums=[int(data[i:i+64],16) for i in range(2,258,64)]
    return {**base,"amounts":[str(x) for x in nums]}


def main() -> None:
    p=argparse.ArgumentParser()
    p.add_argument("--births",type=Path,required=True)
    p.add_argument("--out",type=Path,default=Path("evidence/onchain-birth-followup"))
    args=p.parse_args()
    raw=args.births.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=EXPECTED_BIRTH_FILE_SHA256:
        raise EvidenceError("FIXED_BIRTH_DATASET_SHA_MISMATCH")
    births=[json.loads(line) for line in gzip.decompress(raw).splitlines()]
    if len(births)!=106:
        raise EvidenceError("BIRTH_DATASET_COUNT_MISMATCH")
    eligible=[o for o in births if o.get("quote_core") and o.get("memecoin_candidate")]
    eligible.sort(key=lambda o:hashlib.sha256(o["pool"].encode()).digest())
    selected=eligible[:MAX_POOLS]
    pairs={o["pool"]:o for o in selected}
    out=args.out
    out.mkdir(parents=True,exist_ok=True)
    verified=False
    if rpc("eth_chainId",[]) != "0x38":
        raise EvidenceError("WRONG_CHAIN")
    blocks:dict[int,int]={}
    root_header=header(BLOCK_START,blocks)
    end_header=header(BLOCK_END,blocks)
    spans=[(a,min(a+STEP-1,BLOCK_END)) for a in range(BLOCK_START,BLOCK_END+1,STEP)]
    groups=[list(pairs)[n:n+10] for n in range(0,len(pairs),10)]
    records:dict[str,list[dict[str,Any]]]=defaultdict(list)
    seen:set[tuple[str,int,int]]=set()
    issues=[]
    observations=[]
    for j,(a,b) in enumerate(spans):
        for k,group in enumerate(groups):
            c={"start":a,"end":b,"group":k,"status":"FAILED","n":None}
            try:
                result=rpc("eth_getLogs",[{"fromBlock":hex(a),"toBlock":hex(b),
                         "address":group,"topics":[[SYNC,SWAP]]}])
                if not isinstance(result,list) or len(result)>=5000:
                    raise EvidenceError("CAPPED_OR_INVALID_LOGS")
                for log in result:
                    e=event(log,pairs)
                    if not a<=e["block"]<=b or e["pool"] not in group:
                        raise EvidenceError("LOG_GROUP_MISMATCH")
                    key=(str(e["block_hash"]),e["block"],e["log_index"])
                    if key in seen:
                        continue
                    seen.add(key)
                    records[e["pool"]].append(e)
                c.update({"status":"COMPLETE","n":len(result)})
            except (EvidenceError,ValueError,KeyError,TypeError) as exc:
                c["error"]=str(exc)[:180]
                issues.append(c)
            observations.append(c)
        if (j+1)%20==0:
            print("progress",j+1,"of",len(spans),"events",len(seen),"bad-chunks",len(issues),flush=True)
    (out/"coverage.json").write_text(json.dumps(observations,indent=2)+"\n")
    data={pool:sorted(v,key=lambda x:(x["block"],x["log_index"])) for pool,v in records.items()}
    with gzip.open(out/"v2_raw_events.jsonl.gz","wt",encoding="utf-8") as z:
        for pool in sorted(pairs):
            for obj in data.get(pool,[]):
                z.write(json.dumps(obj,sort_keys=True)+"\n")
    summary:dict[str,Any]={
        "schema":"historical_pancakeswap_v2_native_followup_v1",
        "births_sha256":EXPECTED_BIRTH_FILE_SHA256,"source_births":106,
        "eligible_births":len(eligible),"selected_pairs":len(pairs),
        "window":[BLOCK_START,BLOCK_END],"block_time_sec":[root_header,end_header],
        "ranges_expected":len(spans)*len(groups),"ranges_complete":sum(x["status"]=="COMPLETE" for x in observations),
        "ranges_failed":len(issues),"observed_pairs":len(data),
        "sync_count":sum(x["kind"]=="SYNC" for a in data.values() for x in a),
        "swap_count":sum(x["kind"]=="SWAP" for a in data.values() for x in a),
        "capital_enabled":False,"net_roi":None,"strategy_validated":False,
        "note":"Price ratio only; sellability and execution costs are unknown."}
    prices=[]
    if issues:
        summary["status"]="BLOCKED_INCOMPLETE_RANGE_COVERAGE"
        summary["sample_failure"]=issues[:6]
    else:
        summary["status"]="COMPLETE_FOR_SELECTED_POOLS_AND_RANGE"
        for pool, birth in pairs.items():
            seq=[s for s in data.get(pool,[]) if s["kind"]=="SYNC" and s["block"]>=birth["block"]]
            for s in seq:
                t0,t1=int(s["reserve0"]),int(s["reserve1"])
                quote_is_token0 = birth["token0"]==birth["quote_core"]
                if min(t0,t1)>0:
                    s["ratio"]=(t0/t1) if quote_is_token0 else (t1/t0)
                else:
                    s["ratio"]=None
            birth_ts=header(birth["block"],blocks)
            # Use actual block times; require post-birth observed sync on BOTH sides.
            d5=latest_boundary(birth["block"],BLOCK_END,birth_ts+300,blocks)
            d20=latest_boundary(birth["block"],BLOCK_END,birth_ts+1200,blocks)
            after5=next((s for s in seq if s["block"]>=d5 and s["ratio"] is not None),None)
            after20=next((s for s in seq if s["block"]>=d20 and s["ratio"] is not None),None)
            status="CENSORED"
            move=None
            if after5 and after20:
                # If syncing stops, stale valuations are not silently forward-filled.
                if header(after5["block"],blocks)- (birth_ts+300)<=120 and header(after20["block"],blocks)-(birth_ts+1200)<=120:
                    move=100*(after20["ratio"]/after5["ratio"]-1)
                    status="PRICE_RATIO_PROXY_NOT_EXECUTION"
                else:
                    status="STALE_POST_DECISION_SYNC"
            prices.append({
                "pool":pool,"birth_block":birth["block"],"birth_ts":birth_ts,
                "syncs":len(seq),"swaps":sum(z["kind"]=="SWAP" for z in data.get(pool,[])),
                "status":status,"observed_ratio_pct":move,
                "entry_reserve_quote_raw": str(int(after5["reserve0"]) if birth["token0"]==birth["quote_core"] else int(after5["reserve1"])) if after5 else None,
                "quote_contract":birth["quote_core"],
            })
        summary["num_price_proxies"]=sum(x["observed_ratio_pct"] is not None for x in prices)
        usable=[x["observed_ratio_pct"] for x in prices if x["observed_ratio_pct"] is not None]
        summary["proxy_positive_count"]=sum(x>0 for x in usable)
        summary["proxy_negative_count"]=sum(x<0 for x in usable)
        summary["proxy_mean_pct"]=sum(usable)/len(usable) if usable else None
        summary["proxy_worst_pct"]=min(usable) if usable else None
    (out/"pair_outcomes.json").write_text(json.dumps(prices,sort_keys=True,indent=2,allow_nan=False)+"\n")
    (out/"summary.json").write_text(json.dumps(summary,sort_keys=True,indent=2,allow_nan=False)+"\n")
    print(json.dumps(summary,indent=2),flush=True)


if __name__=="__main__":
    main()
