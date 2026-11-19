"""Causal, position-sized PancakeSwap V2 AMM replay; no signer or wallet.

This is a counterfactual pricing study, NEVER proof a nonstandard token permits
sale. All failures/inactive pools remain counted. HOLDOUT policies frozen using
training cohorts before evaluating later block ranges.
"""

import bisect
import collections
import gzip
import json
import math
import statistics
from pathlib import Path
import argparse

WBNB="0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
TRAIN=(134000000,134050000,134105000,134160000,134225000)
BLIND=(134290000,134350000)
ENTRY_AGE=300
HOLDS=(200,400,800,1600,3000)
FEE=.0025
STAKE_BNB=.005
GAS_BNB=.00003
MIN_Q=.5
ARMS=("ALL","FLOW","FLOW_DEPTH","FLOW_STABLE","EARLY_BUYS","FLOW_SELL_TESTED")

def readgz(path):
    with gzip.open(path,"rt",encoding="utf-8") as f:
        return [json.loads(s) for s in f if s.strip()]

def read_cohorts(root):
    rows=[];summaries=[]
    for start in TRAIN+BLIND:
        folder=root/("cohort-"+str(start))
        summary=json.loads((folder/"summary.json").read_text())
        if summary.get("start")!=start or summary.get("state")!="COMPLETE" or summary.get("n_failed")!=0 or not summary.get("historical_census_valid_in_factory_window"):
            raise ValueError("UNVERIFIED_COHORT_"+str(start))
        births=readgz(folder/"births.jsonl.gz")
        events=readgz(folder/"events.jsonl.gz")
        if len(births)!=summary["pair_births"] or sum(x["kind"]=="SYNC" for x in events)!=summary["event_sync"] or sum(x["kind"]=="SWAP" for x in events)!=summary["event_swap"]:
            raise ValueError("CORRUPT_COHORT_COUNTS_"+str(start))
        groups=collections.defaultdict(list)
        for e in events:
            if not start<=e["block"]<=summary["scan_end"]:
                raise ValueError("EVENT_OUTSIDE_COVERAGE")
            groups[e["pool"]].append(e)
        for b in births:
            if b["quote"]!=WBNB:continue
            ev=sorted(groups[b["pool"]],key=lambda x:(x["block"],x["log_index"]))
            rows.append((start,b,ev,summary["scan_end"]))
        summaries.append(summary)
    return rows,summaries

def reserves(b,e):
    x,y=map(float,e["values"])
    return (x/1e18,y) if b["token0"]==WBNB else (y/1e18,x)

def snapshot(b,events,block):
    past=None
    for e in events:
        if e["block"]>block:break
        if e["kind"]=="SYNC" and e["block"]>=b["birth_block"]: past=e
    return past

def feature(b,events,block):
    s=snapshot(b,events,block)
    if s is None:return None,"NO_INITIAL_LIQUIDITY"
    q,t=reserves(b,s)
    if not math.isfinite(q) or not math.isfinite(t) or q<MIN_Q or t<=0:
        return None,"LOW_OR_ZERO_LIQUIDITY"
    first=next((e for e in events if e["kind"]=="SYNC" and b["birth_block"]<=e["block"]<=block),None)
    first_q=reserves(b,first)[0] if first else 0.
    buys=sells=0
    for e in events:
        if e["block"]>block:break
        if e["block"]<b["birth_block"] or e["kind"]!="SWAP":continue
        v=list(map(int,e["values"]))
        if b["token0"]==WBNB:
            if v[0]>0 and v[3]>0:buys+=1
            if v[1]>0 and v[2]>0:sells+=1
        else:
            if v[1]>0 and v[2]>0:buys+=1
            if v[0]>0 and v[3]>0:sells+=1
    return {"q_bnb":q,"token_raw":t,"buys":buys,"sells":sells,
            "buy_share":buys/max(1,buys+sells),
            "retained_quote":q/first_q if first_q>0 else 0}, "OK"

def selected(arm,ft):
    if arm=="ALL":return True
    if arm=="FLOW":return ft["buys"]>=5 and ft["buy_share"]>=.6
    if arm=="FLOW_DEPTH":return selected("FLOW",ft) and ft["q_bnb"]>=1
    if arm=="FLOW_STABLE":return selected("FLOW_DEPTH",ft) and ft["retained_quote"]>=.9
    if arm=="EARLY_BUYS":return ft["buys"]>=3 and ft["buy_share"]>=.7 and ft["q_bnb"]>=2
    if arm=="FLOW_SELL_TESTED":return selected("FLOW_DEPTH",ft) and ft["sells"]>=2
    raise ValueError("UNKNOWN_RULE")

def multiple(b,events,ft,exit_block):
    s=snapshot(b,events,exit_block)
    if s is None:raise ValueError("MISSING_SYNC")
    q,t=reserves(b,s)
    if min(q,t)<=0:return 0.
    amount_in=STAKE_BNB*(1-FEE)
    token=ft["token_raw"]*amount_in/(ft["q_bnb"]+amount_in)
    if not math.isfinite(token) or token<=0:raise ValueError("INVALID_BUY_AMOUNT")
    sell_in=token*(1-FEE)
    received=q*sell_in/(t+sell_in)
    return received/STAKE_BNB

def after_cost(gross,tax=.05,adverse=.02):
    return gross*(1-tax)**2*(1-adverse)**2-1-GAS_BNB/STAKE_BNB

def describe(rs):
    if not rs:return {"n":0,"mean_pct":None,"median_pct":None,"wr_pct":None}
    return {"n":len(rs),"mean_pct":100*statistics.mean(rs),
            "median_pct":100*statistics.median(rs),
            "wr_pct":100*sum(v>0 for v in rs)/len(rs),
            "min_pct":100*min(rs),"max_pct":100*max(rs),
            "full_loss_count":sum(x<=-.9 for x in rs)}

def build(rows):
    ops=[];counts=collections.Counter()
    for cohort,b,ev,last in rows:
        anchor=b["birth_block"]+ENTRY_AGE
        if anchor>last:counts["NO_ENTRY_COVERAGE"]+=1;continue
        ft,status=feature(b,ev,anchor)
        counts[status]+=1
        if ft is None:continue
        for hold in HOLDS:
            if anchor+hold>last:
                counts["NO_EXIT_COVERAGE"]+=1;continue
            m=multiple(b,ev,ft,anchor+hold)
            ops.append({"cohort":cohort,"pool":b["pool"],"entry_block":anchor,
                        "exit_block":anchor+hold,"hold_blocks":hold,
                        "gross_multiple":m,**ft})
    return ops,dict(counts)

def subset(ops,cohorts,arm,hold):
    return [o for o in ops if o["cohort"] in cohorts and o["hold_blocks"]==hold and selected(arm,o)]

def finite_bank(trades):
    cash=100.0;active=[];peak=100.;max_dd=0.;executed=0;skipped=0
    for t in sorted(trades,key=lambda x:(x["entry_block"],x["pool"])):
        for open_trade in sorted([v for v in active if v[0]<=t["entry_block"]]):
            active.remove(open_trade)
            cash+=open_trade[1]*(1+open_trade[2])
        equity=cash+sum(v[1] for v in active)
        peak=max(peak,equity)
        max_dd=max(max_dd,1-equity/peak)
        if len(active)>=5 or cash<2:
            skipped+=1;continue
        amt=min(.02*equity,cash)
        cash-=amt
        active.append((t["exit_block"],amt,after_cost(t["gross_multiple"])))
        executed+=1
    for open_trade in sorted(active):
        cash+=open_trade[1]*(1+open_trade[2])
        peak=max(peak,cash)
        max_dd=max(max_dd,1-cash/peak)
    return {"initial_usd":100,"final_usd_quote_stable_assumed":cash,
            "executed":executed,"blocked_by_cash_or_concurrency":skipped,
            "drawdown_lower_bound_pct":100*max_dd,
            "caveat":"No actual token taxes/fills, historical BNB/USD variation ignored"}

def reserve_drain_study(rows):
    """Competing hazard: post-entry quote-reserve collapse at any observed Sync.

    This is a liquidity-depth event, not automatically a malicious rug.
    Pools with incomplete follow-up are censored rather than surviving.
    """
    observed=[]
    for cohort,b,events,last in rows:
        t=b["birth_block"]+ENTRY_AGE
        if t>last: continue
        ft,reason=feature(b,events,t)
        if ft is None:continue
        drain_at=None
        for e in events:
            if e["kind"]!="SYNC" or e["block"]<=t:continue
            q,_=reserves(b,e)
            if q <= .10*ft["q_bnb"]:
                drain_at=e["block"]
                break
        observed.append((cohort,t,last,drain_at,ft["sells"]>=2))
    groups={}
    for label,cohortset in (("train",TRAIN),("evaluation",BLIND)):
        subset=[x for x in observed if x[0] in cohortset]
        rowsout={}
        for horizon in (200,400,800,1600,3000):
            comparable=[x for x in subset if x[2]>=x[1]+horizon]
            hits=sum(x[3] is not None and x[3]<=x[1]+horizon for x in comparable)
            sellseen=[x for x in comparable if x[4]]
            sellhits=sum(x[3] is not None and x[3]<=x[1]+horizon for x in sellseen)
            rowsout[str(horizon)]={"at_risk":len(comparable),"depth_collapses":hits,
                "seller_tested_at_risk":len(sellseen),"seller_tested_collapses":sellhits}
        groups[label]={"entry_eligible_pool_count":len(subset),"horizons_blocks":rowsout}
    return groups

def execute(root,out):
    rows,coverage=read_cohorts(root)
    ops,counts=build(rows)
    ranks=[]
    for arm in ARMS:
        for hold in HOLDS:
            a=subset(ops,TRAIN,arm,hold)
            rs=[after_cost(x["gross_multiple"]) for x in a]
            if len(rs)<20:continue
            score=statistics.mean(rs)-1.96*(statistics.stdev(rs)/math.sqrt(len(rs)))
            ranks.append({"arm":arm,"hold_blocks":hold,"train":describe(rs),
                          "score_approx_lower_95":score})
    ranks.sort(key=lambda x:x["score_approx_lower_95"],reverse=True)
    report={
        "schema":"bsc_v2_memecoin_counterfactual_v1",
        "selection":"Training cohorts only, predeclared 6 arms x 5 block horizons",
        "fixed_cohorts_train":list(TRAIN),"fixed_cohorts_blind":list(BLIND),
        "universe_tokens_wbnb":len(rows),
        "all_cohorts":[{"start":x["start"],"pair_births":x["pair_births"],"swaps":x["event_swap"],"syncs":x["event_sync"],"failures":x["n_failed"]} for x in coverage],
        "entry_age_blocks":ENTRY_AGE,"stake_bnb":STAKE_BNB,
        "selection_counts":counts,"train_ranking":ranks,
        "reserve_drain_mechanism":reserve_drain_study(rows),
        "wallet_signer":False,"live_trading":False,"sellable_token_verified":False,
        "economic_status":"NO_GO", "realized_roi":None,
        "limitations":["CP-AMM counterfactual only; unknown transfer taxes/honeypots/failed sales",
         "Gas .00003 BNB is an assumed sensitivity, not historical receipt",
         "Historical block ages are not exact seconds; results reflect ex-post block snapshots",
         "Only seven narrow cohorts, not a full 24-hour/day sampling census",
         "Best-of-30 rules introduces selection bias; two later cohorts are descriptive evaluation"],
    }
    if ranks:
        best=ranks[0]
        test=subset(ops,BLIND,best["arm"],best["hold_blocks"])
        rs=[after_cost(x["gross_multiple"]) for x in test]
        holdout_by_cohort={str(c):describe([after_cost(x["gross_multiple"]) for x in test if x["cohort"]==c]) for c in BLIND}
        report["frozen_rule"]={"arm":best["arm"],"hold_blocks":best["hold_blocks"],"entry_blocks":ENTRY_AGE}
        report["holdout"]={"n":len(test),"net_assumed_cost":describe(rs),
            "per_cohort":holdout_by_cohort,
            "stress_tax_per_side":{str(t):describe([after_cost(x["gross_multiple"],tax=t) for x in test]) for t in [0,.02,.05,.1,.2]},
            "rug_sensitivity_one_extra_total_loss":describe(rs+[-1.006]),
            "optimistic_amm_before_cost":describe([x["gross_multiple"]-1 for x in test]),
            "finite_bank":finite_bank(test),
            "pass_conditions_met":False}
        reason=[]
        if len(test)<100:reason.append("INSUFFICIENT_HOLDOUT_OPPORTUNITIES")
        if not rs or statistics.mean(rs)<=0:reason.append("AFTER_COST_MEAN_NOT_POSITIVE")
        if any(holdout_by_cohort[str(c)]["mean_pct"] is None or holdout_by_cohort[str(c)]["mean_pct"]<=0 for c in BLIND):
            reason.append("NEGATIVE_OR_UNSUPPORTED_HOLDOUT_COHORT")
        reason.append("HISTORICAL_SELLABILITY_AND_TOKEN_TAX_NOT_VERIFIED")
        report["holdout"]["failed_live_gates"]=reason
    else:
        report["holdout"]={"status":"NO_QUALIFYING_TRAINING_STRATEGY"}
    out.mkdir(parents=True,exist_ok=True)
    (out/"economic_report.json").write_text(json.dumps(report,sort_keys=True,indent=2,allow_nan=False)+"\n")
    with gzip.open(out/"asof_trade_candidates.jsonl.gz","wt",encoding="utf-8") as f:
        for row in ops:f.write(json.dumps(row,sort_keys=True)+"\n")
    print(json.dumps({"train_total":len(ranks),"universe":len(rows),
                      "frozen_rule":report.get("frozen_rule"),
                      "holdout":report["holdout"],"state":report["economic_status"]},indent=2,flush=True)

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--cohorts",type=Path,required=True)
    p.add_argument("--out",type=Path,required=True)
    arg=p.parse_args()
    execute(arg.cohorts,arg.out)

if __name__=="__main__":
    main()
