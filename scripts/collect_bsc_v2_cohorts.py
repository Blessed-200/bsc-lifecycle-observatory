"""Causally bounded historical PancakeSwap V2 birth + swap + Sync data acquisition.

Public BSC RPC, no wallet/signer/capital. Every requested block range has
explicit COMPLETE/ERROR status. Never interpret RPC failures as zero events.
"""

import argparse
import gzip
import hashlib
import json
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

RPC = "https://bsc-rpc.publicnode.com"
FACTORY = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73"
CREATED = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
SYNC = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
SWAP = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
USDT = "0x55d398326f99059ff775485246999027b3197955"
SECONDS_BETWEEN_REQUESTS = 0.50
_last_call = 0.0

class EvidenceFailure(Exception):
    pass

def rpc(method, params):
    global _last_call
    body = json.dumps({"jsonrpc":"2.0","id":75,"method":method,"params":params}).encode()
    req = urllib.request.Request(RPC, data=body, method="POST",
                                 headers={"Content-Type":"application/json"})
    for attempt in range(3):
        wait = SECONDS_BETWEEN_REQUESTS - (time.monotonic() - _last_call)
        if wait > 0:
            time.sleep(wait)
        _last_call = time.monotonic()
        try:
            with urllib.request.urlopen(req,timeout=18) as resp:
                raw = resp.read(7_000_001)
            if len(raw) > 7_000_000:
                raise EvidenceFailure("OVERSIZED_RESPONSE")
            doc = json.loads(raw)
            if not isinstance(doc,dict) or doc.get("id") != 75:
                raise EvidenceFailure("BAD_RPC_ENVELOPE")
            if doc.get("result") is None:
                raise EvidenceFailure("EMPTY_RPC_RESULT:"+str(doc.get("error"))[:110])
            return doc["result"]
        except (ValueError, EvidenceFailure, TimeoutError, urllib.error.URLError) as exc:
            if attempt == 2:
                raise EvidenceFailure(str(exc)[:180]) from exc
            time.sleep(1.5*(attempt+1))
    raise AssertionError("unreachable")

def head(block):
    obj=rpc("eth_getBlockByNumber",[hex(block),False])
    if not isinstance(obj,dict) or int(obj.get("number","0x0"),16)!=block or not obj.get("hash"):
        raise EvidenceFailure("INVALID_BLOCK_HEADER")
    return {"block":block,"hash":obj["hash"],"timestamp":int(obj["timestamp"],16)}

def classify_birth(log, start):
    if str(log.get("address","")).lower()!=FACTORY or log.get("removed") is True:
        raise EvidenceFailure("FACTORY_MISMATCH")
    ts=log.get("topics")
    if not isinstance(ts,list) or len(ts)!=3 or str(ts[0]).lower()!=CREATED:
        raise EvidenceFailure("TOPIC_MISMATCH")
    d=str(log.get("data",""))
    if not d.startswith("0x") or len(d)!=130:
        raise EvidenceFailure("FACTORY_DATA_MISMATCH")
    t0="0x"+ts[1][-40:].lower()
    t1="0x"+ts[2][-40:].lower()
    pair="0x"+d[26:66].lower()
    birth=int(log["blockNumber"],16)
    if birth < start:
        raise EvidenceFailure("BIRTH_OUT_OF_SCOPE")
    return {"pool":pair,"token0":t0,"token1":t1,
            "birth_block":birth,"birth_tx":log.get("transactionHash"),
            "birth_log_index":int(log["logIndex"],16),
            "birth_block_hash":log.get("blockHash"),
            "quote": WBNB if WBNB in (t0,t1) and t0!=t1 else
                     (USDT if USDT in (t0,t1) and t0!=t1 else None),
            "cohort_start":start}

def decode_pool_event(log, allowed, lo, hi):
    pool=str(log.get("address","")).lower()
    if pool not in allowed or log.get("removed") is True:
        raise EvidenceFailure("POOL_LOG_IDENTITY")
    topics=log.get("topics") or []
    if not topics or str(topics[0]).lower() not in (SYNC,SWAP):
        raise EvidenceFailure("UNEXPECTED_POOL_EVENT")
    block=int(log["blockNumber"],16)
    if not lo<=block<=hi:
        raise EvidenceFailure("OUT_OF_RANGE_LOG")
    d=str(log.get("data",""))
    topic=str(topics[0]).lower()
    expected=130 if topic==SYNC else 258
    if not d.startswith("0x") or len(d)!=expected:
        raise EvidenceFailure("INVALID_POOL_EVENT_DATA")
    nums=[str(int(d[x:x+64],16)) for x in range(2,expected,64)]
    return {"pool":pool,"block":block,
            "tx_hash":log.get("transactionHash"),"block_hash":log.get("blockHash"),
            "log_index":int(log["logIndex"],16),
            "kind":"SYNC" if topic==SYNC else "SWAP",
            "values":nums}

def json_save(path,obj):
    path.write_text(json.dumps(obj,sort_keys=True,indent=2)+"\n",encoding="utf-8")

def write_gz(path,rows):
    with gzip.open(path,"wt",encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row,sort_keys=True)+"\n")

def query_range(lo,hi,addr,topics):
    return rpc("eth_getLogs",[{"fromBlock":hex(lo),"toBlock":hex(hi),
                              "address":addr,"topics":topics}])

def run(start,out):
    if rpc("eth_chainId",[])!="0x38":
        raise EvidenceFailure("NOT_BSC_MAINNET")
    # Disjoint cohorts chosen before looking at outcomes; last two are evaluation cohorts.
    factory_end=start+3000
    scan_end=start+12000
    header_start=head(start)
    header_end=head(scan_end)
    records=[]
    coverage=[]
    errors=[]
    for i in range(start,factory_end+1,100):
        j=min(factory_end,i+99)
        rec={"stage":"factory","from":i,"to":j,"status":"ERROR"}
        try:
            logs=query_range(i,j,FACTORY,[CREATED])
            if not isinstance(logs,list) or len(logs)>=3000:
                raise EvidenceFailure("FACTORY_LOGS_CAPPED_OR_INVALID")
            decoded=[classify_birth(x,start) for x in logs]
            if any(not i<=x["birth_block"]<=j for x in decoded):
                raise EvidenceFailure("FACTORY_EVENT_OUT_OF_RANGE")
            records.extend(decoded)
            rec.update({"status":"COMPLETE","count":len(logs),
                        "sha":hashlib.sha256(json.dumps(logs,sort_keys=True).encode()).hexdigest()})
        except (EvidenceFailure,ValueError,KeyError,TypeError) as exc:
            rec["error"]=str(exc)[:180]
            errors.append(rec)
        coverage.append(rec)
    pairs=list({x["pool"]:x for x in records}.values())
    if len(pairs)!=len(records):
        errors.append({"stage":"factory","status":"ERROR","error":"DUPLICATE_POOL_BIRTH"})
    # For economics use only WBNB quote; keep all births as a census denominator.
    wbnb=sorted([x for x in pairs if x["quote"]==WBNB],
                key=lambda x:hashlib.sha256(x["pool"].encode()).digest())
    selected=wbnb[:125]
    groups=[[x["pool"] for x in selected[n:n+25]] for n in range(0,len(selected),25)]
    all_events={}
    for a in range(start,scan_end+1,100):
        b=min(scan_end,a+99)
        for index,group in enumerate(groups):
            rec={"stage":"pool","from":a,"to":b,"group":index,
                 "n_addresses":len(group),"status":"ERROR"}
            try:
                logs=query_range(a,b,group,[[SYNC,SWAP]])
                if not isinstance(logs,list) or len(logs)>=3000:
                    raise EvidenceFailure("POOL_LOGS_CAPPED_OR_INVALID")
                decoded=[decode_pool_event(x,set(group),a,b) for x in logs]
                for r in decoded:
                    key=(r["block_hash"],r["log_index"])
                    if key in all_events and all_events[key]!=r:
                        raise EvidenceFailure("CONFLICTING_CHAIN_LOG_IDENTITY")
                    all_events[key]=r
                rec.update({"status":"COMPLETE","count":len(logs),
                            "sha":hashlib.sha256(json.dumps(logs,sort_keys=True).encode()).hexdigest()})
            except (EvidenceFailure,ValueError,KeyError,TypeError) as exc:
                rec["error"]=str(exc)[:180]
                errors.append(rec)
            coverage.append(rec)
        if (a-start)%2000==0:
            print("cohort",start,"block",a,"births",len(records),
                  "events",len(all_events),"errors",len(errors),flush=True)
    out.mkdir(parents=True,exist_ok=True)
    records.sort(key=lambda x:(x["birth_block"],x["birth_log_index"]))
    ev=sorted(all_events.values(),key=lambda x:(x["block"],x["log_index"]))
    write_gz(out/"births.jsonl.gz",records)
    write_gz(out/"events.jsonl.gz",ev)
    write_gz(out/"coverage.jsonl.gz",coverage)
    cc=Counter(x["kind"] for x in ev)
    summary={"schema":"bsc_v2_historical_cohorts_v1","start":start,
             "factory_end":factory_end,"scan_end":scan_end,
             "headers":[header_start,header_end],
             "pair_births":len(records),"wbnb_pairs":len(wbnb),
             "selected_wbnb_pairs":len(selected),"other_births":len(records)-len(wbnb),
             "selected_method":"sha256(pool) deterministic, first 125",
             "event_sync":cc["SYNC"],"event_swap":cc["SWAP"],
             "n_coverage_requests":len(coverage),"n_failed":len(errors),
             "failed_examples":errors[:10],
             "historical_census_valid_in_factory_window":not errors and len(records)>0,
             "state":"COMPLETE" if not errors and len(records)>0 else "BLOCKED",
             "no_real_transactions":True,"wallet_enabled":False}
    json_save(out/"summary.json",summary)
    print(json.dumps(summary,sort_keys=True,indent=2),flush=True)

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--start",type=int,required=True)
    p.add_argument("--out",type=Path,required=True)
    arg=p.parse_args()
    allowed={134000000,134050000,134105000,134160000,134225000,134290000,134350000}
    if arg.start not in allowed:
        raise ValueError("unexpected or unpreregistered cohort")
    run(arg.start,arg.out)

if __name__=="__main__":
    main()
