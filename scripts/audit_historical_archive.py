"""Verify and audit existing BSC lifecycle historical release; read-only, no money.

The dataset is a partially scanned on-chain archive, not a certified census or
historical trading simulator. Never treat missing token histories as winners.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import re
import sqlite3
import tarfile
import tempfile
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

HEX64 = re.compile(r"[0-9a-f]{64}\Z")


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _allowed_member(name: str) -> bool:
    return bool(name) and "/" not in name and "\\" not in name and name not in {".", ".."}


def inspect(pointer: dict[str, Any], archive_file: Path, out: Path) -> dict[str, Any]:
    expected_digest = pointer.get("sha256")
    if not isinstance(expected_digest, str) or not HEX64.fullmatch(expected_digest):
        raise ValueError("head digest invalid")
    if archive_file.name != f"batch-{expected_digest}.tar.gz":
        raise ValueError("head references different release file")
    if sha(archive_file.read_bytes()) != expected_digest:
        raise ValueError("archive SHA256 does not match checkpoint")
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        with tarfile.open(archive_file, "r:gz") as archive:
            members = archive.getmembers()
            total_bytes = sum(m.size for m in members)
            if not members or total_bytes > 512_000_000:
                raise ValueError("oversized/missing members")
            names = [m.name for m in members]
            if len(names) != len(set(names)) or "manifest.json" not in names:
                raise ValueError("manifest absent or duplicate members")
            if any(not m.isfile() or not _allowed_member(m.name) for m in members):
                raise ValueError("unsafe archive member path or type")
            mfile = archive.extractfile("manifest.json")
            if mfile is None:
                raise ValueError("unreadable manifest")
            manifest = json.load(mfile)
            expected = manifest.get("files")
            if not isinstance(expected, dict) or set(expected) | {"manifest.json"} != set(names):
                raise ValueError("manifest not complete")
            raw_counts: Counter[str] = Counter()
            raw_bytes: Counter[str] = Counter()
            for name, entry in expected.items():
                source = archive.extractfile(name)
                if source is None:
                    raise ValueError("missing member")
                body = source.read()
                if len(body) != entry["bytes"] or sha(body) != entry["sha256"]:
                    raise ValueError("member integrity failure: " + name)
                raw_counts[name.split("-", 1)[0]] += 1
                raw_bytes[name.split("-", 1)[0]] += len(body)
                if name == "state.sqlite":
                    (root / "state.sqlite").write_bytes(body)
            if not (root / "state.sqlite").exists():
                raise ValueError("no state.sqlite")
        with sqlite3.connect("file:" + str(root / "state.sqlite") + "?mode=ro", uri=True) as db:
            db.row_factory = sqlite3.Row
            integrity = db.execute("PRAGMA integrity_check").fetchone()
            if not integrity or integrity[0] != "ok":
                raise ValueError("SQLite integrity failure")
            counts = {
                table: int(db.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
                for table in ("tokens", "pools", "seen", "streams", "balances")
            }
            event_counts = {str(x["event"]): int(x["n"]) for x in db.execute(
                "SELECT event, count(*) n FROM seen GROUP BY event"
            )}
            platform_counts = {str(x["platform"]): int(x["n"]) for x in db.execute(
                "SELECT coalesce(platform,'UNKNOWN') platform, count(*) n FROM tokens GROUP BY platform"
            )}
            birth_by_day = {str(x["day"]): int(x["n"]) for x in db.execute(
                "SELECT date(birth_ts,'unixepoch') day, count(*) n "
                "FROM tokens WHERE birth_ts IS NOT NULL GROUP BY day ORDER BY day"
            )}
            ages = db.execute(
                "SELECT min(ts) oldest, max(ts) newest, "
                "sum(CASE WHEN token IS NULL THEN 1 ELSE 0 END) no_token "
                "FROM seen"
            ).fetchone()
            coverage = list(db.execute(
                "SELECT kind,count(*) n, min(start_block) start_block, "
                "min(cursor) minimum_cursor,max(cursor) maximum_cursor "
                "FROM streams GROUP BY kind"
            ))
            meaningful = db.execute(
                "SELECT count(*) n, count(DISTINCT token) tokens "
                "FROM seen WHERE event='Swap' AND token IS NOT NULL"
            ).fetchone()
            unmatched = int(db.execute(
                "SELECT count(*) FROM tokens WHERE birth_ts IS NULL"
            ).fetchone()[0])
            row_count_born = int(db.execute(
                "SELECT count(*) FROM tokens WHERE birth_ts IS NOT NULL"
            ).fetchone()[0])
            out.mkdir(parents=True, exist_ok=True)
            with gzip.open(out / "births.csv.gz", "wt", encoding="utf-8", newline="") as fp:
                wr = csv.writer(fp)
                wr.writerow(("token", "platform", "birth_block", "birth_ts", "creator", "first_observed_block"))
                for row in db.execute(
                    "SELECT address,platform,birth_block,birth_ts,creator,first_block "
                    "FROM tokens ORDER BY coalesce(birth_ts,0),address"
                ):
                    wr.writerow(tuple(row))
            summary = {
                "schema": "bsc_archive_audit_v1",
                "source_archive_sha256": expected_digest,
                "timestamp_utc": datetime.now(UTC).isoformat(),
                "archive_size_bytes": archive_file.stat().st_size,
                "verified_uncompressed_bytes": total_bytes,
                "archive_files_by_kind": dict(raw_counts),
                "state_counts": counts,
                "tokens_with_birth_ts": row_count_born,
                "tokens_without_birth_ts": unmatched,
                "births_by_day": birth_by_day,
                "platform_counts": platform_counts,
                "event_counts": event_counts,
                "seen_time_range": {"min": ages["oldest"], "max": ages["newest"],
                                    "without_token": ages["no_token"]},
                "stream_coverage": [dict(row) for row in coverage],
                "swaps_with_token_identity": int(meaningful["n"]),
                "tokens_with_swap_token_identity": int(meaningful["tokens"]),
                "warnings": [
                    "The archive combines collected logs and partially advanced stream cursors; not 24h exhaustive",
                    "Swaps do not automatically equal executable entries; token identity may be missing",
                    "No post-birth event timestamp is assumed live when received via historical backfill",
                    "No historical trade execution, token taxes, gas or sellability reconstructed",
                    "Transfer-event wallet balances may be inconsistent due to incomplete history",
                ],
                "strategy_validated": False,
                "historical_roi_net": None,
                "capital_enabled": False,
                "mode": "ARCHIVE_INSPECTION_ONLY",
            }
            (out / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
            print(json.dumps({k: summary[k] for k in (
                "state_counts", "tokens_with_birth_ts", "tokens_without_birth_ts",
                "seen_time_range", "swaps_with_token_identity", "mode"
            )}, indent=2), flush=True)
            return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--head", required=True, type=Path)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--out", default="evidence/lifecycle-archive", type=Path)
    args = parser.parse_args()
    pointer = json.loads(args.head.read_text())
    inspect(pointer, args.archive, args.out)


if __name__ == "__main__":
    main()
