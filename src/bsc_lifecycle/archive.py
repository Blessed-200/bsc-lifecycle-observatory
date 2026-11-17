"""Immutable daily release archives, compare-and-swap head and verified restart."""

from __future__ import annotations

import base64
import json
import re
import shutil
import sqlite3
import subprocess
import tarfile
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bsc_lifecycle.store import Journal, Store, atomic, encoded, sha

BRANCH = "lifecycle-state"
HEAD_FILE = "lifecycle-head.json"


class TransportError(Exception):
    pass


class Github:
    def __init__(self, repo: str) -> None:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
            raise ValueError("invalid archive repository")
        self.repo = repo
        executable = shutil.which("gh")
        if not executable:
            raise TransportError("GitHub CLI unavailable")
        self.executable = executable
        self.revision: str | None = None
        self.parent: dict[str, Any] | None = None

    def command(self, *args: str, missing_ok: bool = False) -> str | None:
        result = subprocess.run([self.executable, *args], capture_output=True, timeout=240, check=False)  # noqa: S603
        if result.returncode:
            if missing_ok and b"HTTP 404" in result.stderr:
                return None
            raise TransportError(f"GitHub {args[0]} failed; cursor publication not acknowledged")
        return result.stdout.decode()

    def api(self, path: str, payload: dict[str, Any] | None = None, *, missing_ok: bool = False) -> Any:
        endpoint = f"repos/{self.repo}" + ("/" + path if path else "")
        if payload is None:
            raw = self.command("api", endpoint, missing_ok=missing_ok)
        else:
            with tempfile.TemporaryDirectory() as temp:
                file = Path(temp) / "request.json"
                file.write_bytes(encoded(payload))
                verb = "PUT" if path.startswith("contents/") else "POST"
                raw = self.command("api", "--method", verb, endpoint, "--input", str(file))
        return json.loads(raw) if raw else None

    def head(self, *, bootstrap: bool = False) -> dict[str, Any] | None:
        info = self.api("")  # authenticate access; an unavailable repository is not an empty history
        branch = self.api(f"git/ref/heads/{BRANCH}", missing_ok=True)
        if branch is None:
            if not bootstrap:
                raise TransportError("archive branch absent; explicit first-run bootstrap required")
            main = self.api(f"git/ref/heads/{info['default_branch']}")
            self.api("git/refs", {"ref": "refs/heads/" + BRANCH, "sha": main["object"]["sha"]})
        record = self.api(f"contents/{HEAD_FILE}?ref={BRANCH}", missing_ok=True)
        if record is None:
            if not bootstrap:
                raise TransportError("checkpoint head absent; refusing to reset collection")
            return None
        self.revision = record["sha"]
        self.parent = json.loads(base64.b64decode(record["content"], validate=False))
        return self.parent

    def download(self, pointer: dict[str, Any], directory: Path) -> Path:
        if not re.fullmatch(r"lifecycle-\d{4}-\d{2}-\d{2}", pointer["tag"]):
            raise TransportError("invalid archive partition")
        expected = f"batch-{pointer['sha256']}.tar.gz"
        if pointer.get("asset") != expected or not re.fullmatch(r"[a-f0-9]{64}", pointer["sha256"]):
            raise TransportError("invalid archive identity")
        self.command(
            "release", "download", pointer["tag"], "--repo", self.repo, "--pattern", expected, "--dir", str(directory)
        )
        path = directory / expected
        if sha(path.read_bytes()) != pointer["sha256"]:
            raise TransportError("remote archive digest mismatch")
        return path

    def publish(self, path: Path, pointer: dict[str, Any], status: dict[str, Any]) -> None:
        if self.api("releases/tags/" + pointer["tag"], missing_ok=True) is None:
            self.api(
                "releases",
                {
                    "tag_name": pointer["tag"],
                    "name": "BSC raw research " + pointer["tag"],
                    "body": (
                        "Immutable raw collection batches, state and coverage manifests. "
                        "No expiry policy. No trading performance claim."
                    ),
                    "make_latest": "false",
                },
            )
        self.command("release", "upload", pointer["tag"], str(path), "--repo", self.repo)
        with tempfile.TemporaryDirectory() as temp:
            restored = self.download(pointer, Path(temp))
            inspect_archive(restored)
        payload = {
            "message": "Commit verified BSC collection checkpoint",
            "branch": BRANCH,
            "content": base64.b64encode(encoded({**pointer, "status": status})).decode(),
        }
        if self.revision:
            payload["sha"] = self.revision
        response = self.api("contents/" + HEAD_FILE, payload)
        self.revision = response["content"]["sha"]
        self.parent = pointer


def inspect_archive(path: Path, restore_db: Path | None = None) -> dict[str, Any]:
    with tarfile.open(path, "r:gz") as archive:
        members = archive.getmembers()
        if sum(m.size for m in members) > 512_000_000:
            raise TransportError("archive uncompressed limit exceeded")
        names = [m.name for m in members]
        if len(names) != len(set(names)) or "manifest.json" not in names:
            raise TransportError("duplicate or missing manifest")
        if any(not m.isfile() or Path(m.name).name != m.name for m in members):
            raise TransportError("unsafe archive member")
        manifest_file = archive.extractfile("manifest.json")
        if manifest_file is None:
            raise TransportError("missing manifest bytes")
        manifest = json.load(manifest_file)
        if manifest.get("version") != 1 or set(names) != set(manifest["files"]) | {"manifest.json"}:
            raise TransportError("manifest member mismatch")
        db_bytes = None
        for name, expected in manifest["files"].items():
            member = archive.extractfile(name)
            if member is None:
                raise TransportError("missing archive bytes")
            data = member.read()
            if len(data) != expected["bytes"] or sha(data) != expected["sha256"]:
                raise TransportError("archive file digest mismatch")
            if name == "state.sqlite":
                db_bytes = data
        if db_bytes is None:
            raise TransportError("missing state database")
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "state.sqlite"
            db.write_bytes(db_bytes)
            with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as connection:
                if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise TransportError("restored database corrupt")
                row = connection.execute("SELECT value FROM meta WHERE key='version'").fetchone()
                if row is None or json.loads(row[0]) != 1:
                    raise TransportError("restored database version mismatch")
            if restore_db is not None:
                if restore_db.exists():
                    raise TransportError("refusing to overwrite local state")
                atomic(restore_db, db_bytes)
        return dict(manifest)


def restore(transport: Github, path: Path, *, bootstrap: bool = False) -> None:
    pointer = transport.head(bootstrap=bootstrap)
    if pointer is None:
        return
    with tempfile.TemporaryDirectory() as temp:
        archive = transport.download(pointer, Path(temp))
        inspect_archive(archive, path)


def publish(
    store: Store, journal: Journal, directory: Path, transport: Github, provenance: dict[str, Any]
) -> dict[str, Any]:
    journal.rotate()
    raw_files = [p for p in sorted(directory.glob("raw-*.jsonl")) if p != journal.path]
    status = store.status()
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        store.snapshot(root / "state.sqlite")
        (root / "status.json").write_bytes(encoded(status))
        files = [root / "state.sqlite", root / "status.json", *raw_files]
        manifest = {
            "version": 1,
            "created_at": time.time(),
            "parent": transport.parent,
            "provenance": provenance,
            "files": {p.name: {"sha256": sha(p.read_bytes()), "bytes": p.stat().st_size} for p in files},
        }
        (root / "manifest.json").write_bytes(encoded(manifest))
        archive = root / "batch.tar.gz"
        with tarfile.open(archive, "w:gz", compresslevel=3) as bundle:
            for file in [*files, root / "manifest.json"]:
                bundle.add(file, arcname=file.name, recursive=False)
        fingerprint = sha(archive.read_bytes())
        named = root / f"batch-{fingerprint}.tar.gz"
        archive.rename(named)
        pointer = {
            "version": 1,
            "tag": "lifecycle-" + datetime.now(UTC).strftime("%Y-%m-%d"),
            "asset": named.name,
            "sha256": fingerprint,
            "created_at": time.time(),
        }
        transport.publish(named, pointer, status)
    # Acknowledged only after upload, independent download, manifest validation and CAS head update.
    for file in raw_files:
        file.unlink()
    return {**pointer, "status": status}
