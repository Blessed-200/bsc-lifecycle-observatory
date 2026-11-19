"""Extract and replay observed Sync reserve ratios from a verified BSC archive.

Read only: public historical logs, no trade signing. Reserve ratio is not a
realizable return or USD price; no claim about sellability.
"""

from __future__ import annotations

import argparse
import base64
import csv
import gzip
import hashlib
import json
import math
import sqlite3
import tarfile
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

SYNC = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
SWAP = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
CORE = {
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
    "0x55d398326f99059ff775485246999027b3197955",
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",
}


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def report(pointer: dict[str, Any], path: Path, out: Path) -> None:
    if path.name != "batch-" + pointer["sha256"] + ".tar.gz" or digest(path.read_bytes()) != pointer["sha256"]:
        raise ValueError("checkpoint digest failure")
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        with tarfile.open(path, "r:gz") as z:
            manifest_stream = z.extractfile("manifest.json")
            if manifest_stream is None:
                raise ValueError("missing manifest")
            manifest = json.load(manifest_stream)
            files = manifest.get("files") or {}
            names = [x.name for x in z.getmembers()]
            if set(names) != set(files) | {"manifest.json"} or len(names) != len(set(names)):
                raise ValueError("archive mismatch")
            for entry in z.getmembers():
                if not entry.isfile() or "/" in entry.name or "\\" in entry.name:
                    raise ValueError("unsafe archive entry")
            dbmember = z.extractfile("state.sqlite")
            if dbmember is None:
                raise ValueError("no SQLite state")
            dbbytes = dbmember.read()
            if digest(dbbytes) != files["state.sqlite"]["sha256"]:
                raise ValueError("unverified SQLite")
            (root / "state.sqlite").write_bytes(dbbytes)
            with sqlite3.connect("file:" + str(root / "state.sqlite") + "?mode=ro", uri=True) as db:
                if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise ValueError("corrupt SQLite")
                pools = {
                    str(a).lower(): (str(b).lower(), str(c).lower(), str(k))
                    for a, k, b, c in db.execute("SELECT address,kind,token0,token1 FROM pools")
                }
                observed: dict[str, list[tuple[int, int, int, float, int, int]]] = defaultdict(list)
                counters: Counter[str] = Counter()
                dedup: dict[str, tuple[str, str]] = {}
                for name in files:
                    if not name.startswith("raw-") or not name.endswith(".jsonl"):
                        continue
                    source = z.extractfile(name)
                    if source is None:
                        raise ValueError("raw member missing")
                    raw = source.read()
                    if digest(raw) != files[name]["sha256"]:
                        raise ValueError("unverified raw journal")
                    for line in raw.splitlines():
                        entry = json.loads(line)
                        if entry.get("kind") != "http" or entry.get("method") != "POST":
                            continue
                        request = entry.get("request")
                        if not isinstance(request, dict) or request.get("method") != "eth_getLogs":
                            continue
                        try:
                            data = base64.b64decode(entry["body_b64"], validate=True)
                        except (ValueError, KeyError) as exc:
                            raise ValueError("invalid retained HTTP body") from exc
                        if digest(data) != entry["body_sha256"]:
                            raise ValueError("tampered retained HTTP body")
                        if entry.get("status") != 200 or entry.get("truncated"):
                            counters["http_not_usable"] += 1
                            continue
                        body = json.loads(data)
                        logs = body.get("result")
                        if not isinstance(logs, list):
                            counters["rpc_without_log_array"] += 1
                            continue
                        counters["rpc_batches"] += 1
                        for log in logs:
                            if not isinstance(log, dict):
                                continue
                            addr = str(log.get("address", "")).lower()
                            topics = log.get("topics") or []
                            if not topics:
                                continue
                            topic = str(topics[0]).lower()
                            if topic not in (SYNC, SWAP):
                                continue
                            block_hash = str(log.get("blockHash", ""))
                            log_index = str(log.get("logIndex", ""))
                            if not block_hash.startswith("0x") or not log_index.startswith("0x"):
                                counters["bad_log_identity"] += 1
                                continue
                            key = block_hash + ":" + log_index
                            current = (addr, str(log.get("data")))
                            if key in dedup:
                                if dedup[key] != current:
                                    raise ValueError("conflicting duplicate log")
                                continue
                            dedup[key] = current
                            if addr not in pools:
                                counters["pool_unknown"] += 1
                                continue
                            if topic == SWAP:
                                counters["swap_logs"] += 1
                                continue
                            t0, t1, kind = pools[addr]
                            if kind not in ("pool_v2", "pancake_v2", "v2"):
                                counters["not_v2"] += 1
                                continue
                            if (t0 in CORE) == (t1 in CORE):
                                counters["no_single_known_quote"] += 1
                                continue
                            raw_data = str(log.get("data", ""))
                            if not raw_data.startswith("0x") or len(raw_data) < 130:
                                counters["short_sync"] += 1
                                continue
                            reserve0 = int(raw_data[2:66], 16)
                            reserve1 = int(raw_data[66:130], 16)
                            if reserve0 == 0 or reserve1 == 0:
                                counters["zero_reserve"] += 1
                                continue
                            # ratio of quote reserve to token reserve; decimals cancel in RETURNS.
                            ratio = (reserve0 / reserve1) if t0 in CORE else (reserve1 / reserve0)
                            if not math.isfinite(ratio) or ratio <= 0:
                                counters["invalid_ratio"] += 1
                                continue
                            block = int(log["blockNumber"], 16)
                            idx = int(log_index, 16)
                            seen_row = db.execute("SELECT ts FROM seen WHERE id=?", (key,)).fetchone()
                            timestamp = int(seen_row[0]) if seen_row and seen_row[0] is not None else 0
                            observed[addr].append((block, idx, timestamp, ratio, reserve0, reserve1))
                            counters["sync_ratios_observed"] += 1
                out.mkdir(parents=True, exist_ok=True)
                summary: dict[str, Any] = {
                    "schema": "bsc_v2_amm_ratio_tape_v1",
                    "archive_sha256": pointer["sha256"],
                    "token_execution_or_sale_verified": False,
                    "economic_net_roi": None,
                    "mode": "NON_EXECUTABLE_RESERVE_RATIO_ONLY",
                    "raw_counts": dict(counters),
                    "n_pools_with_observed_sync": len(observed),
                }
                studied: list[dict[str, Any]] = []
                with gzip.open(out / "v2_sync_reserve_ratios.csv.gz", "wt", newline="") as handle:
                    writer = csv.writer(handle)
                    writer.writerow(("pool", "block", "log_index", "ts_from_seen_or_zero",
                                     "reserve_ratio", "reserve0_raw", "reserve1_raw"))
                    for pool, data in sorted(observed.items()):
                        seq = sorted(set(data), key=lambda x: (x[0], x[1]))
                        for row in seq:
                            writer.writerow((pool, *row))
                        # This is a raw ratio change, not hypothetical execution.
                        if len(seq) > 1 and seq[0][3] > 0:
                            change = 100 * (seq[-1][3] / seq[0][3] - 1)
                            if math.isfinite(change):
                                studied.append({"pool": pool, "syncs": len(seq),
                                                "raw_reserve_ratio_change_pct": change,
                                                "known_timestamp_count": sum(x[2] > 0 for x in seq)})
                summary["n_pools_two_or_more_syncs"] = len(studied)
                summary["ratio_change_descriptive"] = {
                    "min": min((r["raw_reserve_ratio_change_pct"] for r in studied), default=None),
                    "max": max((r["raw_reserve_ratio_change_pct"] for r in studied), default=None),
                    "positive": sum(r["raw_reserve_ratio_change_pct"] > 0 for r in studied),
                    "negative": sum(r["raw_reserve_ratio_change_pct"] < 0 for r in studied),
                }
                summary["limitations"] = [
                    "This is an archive slice and cannot reconstruct excluded/earlier or future trades",
                    "Reserves are NOT fills; transaction taxes, gas, MEV, reverts, honeypots are unknown",
                    "Rate of change compares first and last OBSERVED sync in each pool; cannot infer a holding period",
                    "A ratio in BNB is not a USD price. Asset decimals cancel only for relative percentage change",
                    "Tokens with no Sync are retained in the archived state but not in this price-ratio tape",
                ]
                (out / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
                print(json.dumps(summary, indent=2), flush=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--head", type=Path, required=True)
    p.add_argument("--archive", type=Path, required=True)
    p.add_argument("--out", type=Path, default=Path("evidence/amm-tape"))
    a = p.parse_args()
    report(json.loads(a.head.read_text()), a.archive, a.out)


if __name__ == "__main__":
    main()
