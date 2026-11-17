"""Public early-buyer observations, never trading, signing or wallet P/L analysis."""

from __future__ import annotations

import argparse
import asyncio
import json
import tarfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import aiohttp

from bsc_lifecycle.archive import Github, restore
from bsc_lifecycle.codec import address
from bsc_lifecycle.network import Reader, ReadError
from bsc_lifecycle.store import Journal, Store, encoded

SOURCES = ("gmgn_early", "ave_early")


def early_url(source: str, token: str) -> str:
    token = address(token)
    if source == "gmgn_early":
        return "https://gmgn.ai/api/v1/tokens/top_buyers/bsc/" + token
    if source == "ave_early":
        return "https://dingquanyu.com/v1api/v3/stats/earlyholders?" + urlencode({"token_id": token + "-bsc"})
    raise ValueError("unsupported read-only source")


def parse_buyers(source: str, response: Any) -> list[dict[str, str]]:
    if not isinstance(response, dict):
        raise ReadError("early buyers: response not an object")
    if source == "gmgn_early":
        if response.get("code") != 0 or not isinstance(response.get("data"), dict):
            raise ReadError("GMGN early buyer response unavailable")
        holders = response["data"].get("holders")
        if not isinstance(holders, dict):
            raise ReadError("GMGN early buyer schema changed")
        rows = holders.get("holderInfo", []) if not holders or "holderInfo" in holders else None
        wallet_field = "wallet_address"
    elif source == "ave_early":
        if response.get("status") != 1:
            raise ReadError("Ave early buyer response unavailable")
        rows, wallet_field = response.get("data"), "account_address"
    else:
        raise ValueError("unsupported read-only source")
    if not isinstance(rows, list):
        raise ReadError("early buyer list missing; not an empty history")
    result: dict[str, dict[str, str]] = {}
    for row in rows:
        try:
            wallet = address(row[wallet_field])
        except (ValueError, KeyError, TypeError) as exc:
            raise ReadError("invalid early buyer identity") from exc
        state = row.get("status", row.get("tag", "unknown"))
        result[wallet] = {"wallet": wallet, "provider_state": str(state)[:128]}
    return list(result.values())


def inspect_activity(response: Any, wallets: set[str]) -> dict[str, Any]:
    if not isinstance(response, dict) or response.get("code") != 0 or not isinstance(response.get("data"), dict):
        raise ReadError("GMGN activity unavailable")
    data = response["data"]
    history = data.get("history")
    if not isinstance(history, list):
        raise ReadError("GMGN activity schema changed")
    for row in history:
        try:
            if address(row["maker"]) not in wallets:
                raise ReadError("GMGN wallet filter not honored")
        except (ValueError, KeyError, TypeError) as exc:
            raise ReadError("GMGN activity identity missing") from exc
    return {
        "records": len(history),
        "more_pages": bool(data.get("next")),
        "complete_history": False,
        "basis": "provider_claim; transaction attribution not independently verified",
    }


async def collect(store: Store, journal: Journal, reader: Reader, *, limit: int = 5) -> dict[str, Any]:
    if not 1 <= limit <= 5:
        raise ValueError("source batch must be 1..5")
    store.db.executescript("""
        CREATE TABLE IF NOT EXISTS wallet_poll(
          token TEXT NOT NULL,source TEXT NOT NULL,last_attempt REAL NOT NULL,last_success REAL,
          last_count INTEGER,status TEXT NOT NULL,PRIMARY KEY(token,source));
        CREATE TABLE IF NOT EXISTS early_wallets(
          token TEXT NOT NULL,source TEXT NOT NULL,wallet TEXT NOT NULL,first_seen REAL NOT NULL,
          last_seen REAL NOT NULL,provider_state TEXT NOT NULL,PRIMARY KEY(token,source,wallet));
    """)
    health = store.get("wallet_source_health") or {}
    for source in SOURCES:
        now = time.time()
        rows = store.db.execute(
            """SELECT t.address FROM tokens t LEFT JOIN wallet_poll w ON w.token=t.address AND w.source=?
            WHERE t.birth_ts IS NOT NULL AND coalesce(w.last_attempt,0) < ? -
            CASE WHEN t.birth_ts>? THEN 300 WHEN t.birth_ts>? THEN 3600 ELSE 86400 END
            ORDER BY coalesce(w.last_attempt,0),t.birth_ts,t.address LIMIT ?""",
            (source, now, now - 3600, now - 86400, limit),
        ).fetchall()
        for row in rows:
            token = row[0]
            await asyncio.sleep(3.1)
            observed_at = time.time()
            try:
                response = await reader.request(source, early_url(source, token))
                buyers = parse_buyers(source, response)
            except ReadError:
                journal.write("wallet_source_failure", {"source": source, "token": token})
                journal.flush()
                with store.db:
                    store.quality(source + "_read_failure")
                    store.db.execute(
                        """INSERT INTO wallet_poll VALUES (?,?,?,NULL,NULL,'UNAVAILABLE')
                        ON CONFLICT(token,source) DO UPDATE SET
                        last_attempt=excluded.last_attempt,status='UNAVAILABLE'""",
                        (token, source, observed_at),
                    )
                    health[source] = {"status": "UNAVAILABLE", "last_attempt": observed_at}
                    store.set("wallet_source_health", health)
                break
            journal.write(
                "early_buyer_observation",
                {
                    "source": source,
                    "token": token,
                    "buyers": buyers,
                    "observed_at": observed_at,
                    "complete_history": False,
                    "rank_is_transaction_order": False,
                    "basis": "provider classification, not proof of skill or independent identities",
                },
            )
            journal.flush()
            with store.db:
                store.db.execute(
                    "INSERT OR REPLACE INTO wallet_poll VALUES (?,?,?,?,?,?)",
                    (token, source, observed_at, observed_at, len(buyers), "OK" if buyers else "NO_DATA"),
                )
                for buyer in buyers:
                    store.db.execute(
                        """INSERT INTO early_wallets VALUES (?,?,?,?,?,?) ON CONFLICT(token,source,wallet)
                        DO UPDATE SET last_seen=excluded.last_seen,provider_state=excluded.provider_state""",
                        (token, source, buyer["wallet"], observed_at, observed_at, buyer["provider_state"]),
                    )
                health[source] = {
                    "status": "OK" if buyers else "NO_DATA",
                    "last_success": observed_at,
                    "last_token_wallets": len(buyers),
                    "complete_history": False,
                    "authentication": "anonymous_public_read",
                }
                store.set("wallet_source_health", health)
            if buyers:
                await asyncio.sleep(3.1)
                wallets = {b["wallet"] for b in buyers}
                query = urlencode({"limit": 100, "makers": ",".join(sorted(wallets))})
                try:
                    response = await reader.request(
                        "gmgn_activity", "https://gmgn.ai/vas/api/v1/token_trades/bsc/" + token + "?" + query
                    )
                    evidence = inspect_activity(response, wallets)
                    journal.write("wallet_activity_observation", {"token": token, "wallet_source": source, **evidence})
                except ReadError:
                    store.quality("gmgn_activity_read_failure")
                    journal.write("wallet_activity_failure", {"token": token, "wallet_source": source})
                journal.flush()
                store.db.commit()
    return dict(health)


def probe(args: argparse.Namespace) -> None:
    """Read a copy of collector state; never publish or change its cursor."""
    root: Path = args.out
    root.mkdir(parents=True, exist_ok=True)
    restore(Github(args.repo), root / "state.sqlite")
    store, journal = Store(root / "state.sqlite"), Journal(root)
    journal.write("wallet_source_probe", {"purpose": "connectivity_and_schema_only", "outcomes_evaluated": False})

    async def read_sources() -> dict[str, Any]:
        async with aiohttp.ClientSession(trust_env=args.trust_env, timeout=aiohttp.ClientTimeout(total=20)) as session:
            return await collect(store, journal, Reader(session, journal), limit=2)

    health = asyncio.run(read_sources())
    journal.flush()
    journal.file.close()
    store.db.close()
    summary = {
        "created_at": time.time(),
        "sources": health,
        "wallet_connected": False,
        "outcomes_evaluated": False,
        "collector_state_modified": False,
    }
    (root / "source-summary.json").write_bytes(encoded(summary))
    with tarfile.open(root / "source-probe.tar.gz", "w:gz") as tar:
        for file in [*root.glob("raw-*.jsonl"), root / "source-summary.json"]:
            tar.add(file, arcname=file.name, recursive=False)
    print(json.dumps(summary, sort_keys=True))
    if any(health.get(source, {}).get("status") not in {"OK", "NO_DATA"} for source in SOURCES):
        raise SystemExit(2)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--trust-env", action="store_true")
    probe(parser.parse_args())


if __name__ == "__main__":
    main()
