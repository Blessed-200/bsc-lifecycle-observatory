"""Audit collection metadata without evaluating prices, returns or strategy outcomes."""

from __future__ import annotations

import argparse
import base64
import json
import sqlite3
import tarfile
import tempfile
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from bsc_lifecycle.archive import BRANCH, HEAD_FILE, Github, TransportError, inspect_archive
from bsc_lifecycle.store import atomic, encoded

REPORT_BRANCH = "lifecycle-reports"
STATE_FILE = "monitor-state.json"
ZONE = ZoneInfo("America/Havana")
MAX_BATCHES = 48


def read_json(transport: Github, path: str, branch: str, *, missing_ok: bool = False) -> Any:
    record = transport.api(f"contents/{path}?ref={branch}", missing_ok=missing_ok)
    if record is None:
        return None
    if record.get("encoding") != "base64" or not record.get("content"):
        raise TransportError("report file is not an inline JSON document")
    return json.loads(base64.b64decode(record["content"]))


def read_archive(path: Path, temp: Path) -> dict[str, Any]:
    """Verify bytes, then read only metadata, timestamps, coverage and error counts."""
    db_path = temp / "audit.sqlite"
    manifest = inspect_archive(path, db_path)
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as db:
        meta = {k: json.loads(v) for k, v in db.execute("SELECT key,value FROM meta")}
        streams = {
            name: {"cursor": cursor, "start": start, "covered_at": meta.get("covered_time:" + name)}
            for name, cursor, start in db.execute("SELECT name,cursor,start_block FROM streams")
        }
        tokens = [
            {"address": a, "birth": birth, "birth_block": block, "stream": stream, "start": start}
            for a, birth, block, stream, start in db.execute(
                """SELECT t.address,t.birth_ts,t.birth_block,e.stream,e.start_block FROM tokens t
                LEFT JOIN entities e ON e.address=t.address WHERE t.birth_ts IS NOT NULL"""
            )
        ]
        pools: dict[str, list[dict[str, Any]]] = {}
        for t0, t1, birth, stream in db.execute(
            "SELECT p.token0,p.token1,p.birth_block,e.stream FROM pools p LEFT JOIN entities e ON e.address=p.address"
        ):
            for token in (t0, t1):
                pools.setdefault(token, []).append({"birth_block": birth, "stream": stream})
    db_path.unlink()
    records: Counter[str] = Counter()
    errors: Counter[str] = Counter()
    source_requests: Counter[str] = Counter()
    ranges = []
    received = []
    with tarfile.open(path) as tar:
        status_file = tar.extractfile("status.json")
        if status_file is None:
            raise TransportError("missing archived status")
        status = json.load(status_file)
        for name in manifest["files"]:
            if not name.endswith(".jsonl"):
                continue
            raw = tar.extractfile(name)
            if raw is None:
                raise TransportError("missing raw journal")
            for line in raw:
                row = json.loads(line)
                kind = row["kind"]
                records[kind] += 1
                received.append(float(row["received_at"]))
                if kind == "coverage":
                    stream = streams.get(row["stream"])
                    if stream and row["to_block"] <= stream["cursor"]:
                        ranges.append({k: row[k] for k in ("stream", "from_block", "to_block")})
                    else:
                        errors["coverage_not_committed"] += 1
                elif kind == "http":
                    source_requests[row["source"]] += 1
                    if row.get("status") != 200:
                        errors[f"http_{row.get('status', 'unknown')}"] += 1
                    if row.get("truncated"):
                        errors["explicitly_truncated_response"] += 1
                elif kind in {"http_failure", "cycle_failure", "session_failure"}:
                    errors[kind] += 1
    return {
        "manifest": manifest,
        "status": status,
        "streams": streams,
        "tokens": tokens,
        "pools": pools,
        "ranges": ranges,
        "records": dict(records),
        "errors": dict(errors),
        "source_requests": dict(source_requests),
        "received_from": min(received) if received else None,
        "received_to": max(received) if received else None,
    }


def assess(
    batches: list[dict[str, Any]], previous: dict[str, Any] | None, *, reached_baseline: bool, now: float
) -> tuple[dict[str, Any], dict[str, int]]:
    """Batches ordered oldest first. No sampling of return or wallet P/L fields."""
    if not batches:
        raise ValueError("an archive is required")
    latest = batches[-1]
    status = latest["status"]
    cursors = dict((previous or {}).get("coverage_cursors", {})) if reached_baseline else {}
    ranges_checked = 0
    gaps = []
    errors: Counter[str] = Counter()
    requests: Counter[str] = Counter()
    for batch in batches:
        errors.update(batch["errors"])
        requests.update(batch["source_requests"])
        for r in batch["ranges"]:
            stream = r["stream"]
            default = batch["streams"][stream]["start"] - 1
            last = cursors.get(stream, default)
            if r["from_block"] > last + 1:
                gaps.append({"stream": stream, "from_block": last + 1, "to_block": r["from_block"] - 1})
            if r["to_block"] > last:
                cursors[stream] = r["to_block"]
            ranges_checked += 1
        for stream, info in batch["streams"].items():
            verified = cursors.get(stream, info["start"] - 1)
            if verified != info["cursor"]:
                gaps.append({"stream": stream, "verified_cursor": verified, "database_cursor": info["cursor"]})
    prior_unverified = bool((previous or {}).get("continuity_unverified", False))
    continuity_ok = reached_baseline and not gaps and not prior_unverified
    covered = status.get("protocol_covered_until")
    matured = 0
    complete = 0
    for token in latest["tokens"]:
        birth = token["birth"]
        if birth + 86400 > status["updated_at"]:
            continue
        matured += 1
        stream = latest["streams"].get(token["stream"], {})
        if not covered or covered < birth + 86400 or (stream.get("covered_at") or 0) < birth + 86400:
            continue
        if token["start"] is None or token["start"] > token["birth_block"]:
            continue
        # Conservatively require every discovered pool's stream through the 24h boundary.
        if any(
            (latest["streams"].get(pool["stream"], {}).get("covered_at") or 0) < birth + 86400
            for pool in latest["pools"].get(token["address"], [])
        ):
            continue
        complete += 1
    quality = status.get("quality", {})
    old_quality = (previous or {}).get("quality", {})
    delta = {k: v - old_quality.get(k, 0) for k, v in quality.items() if v > old_quality.get(k, 0)}
    alarms = []
    if now - status["updated_at"] > 1800:
        alarms.append("Sin archivo nuevo en más de 30 minutos")
    if status["updated_at"] > now + 60:
        alarms.append("Hora del archivo incompatible con el reloj")
    if covered is None or now - covered > 1200:
        alarms.append("La lectura del protocolo está retrasada más de 20 minutos o no consta")
    entity_times = [s["covered_at"] for n, s in latest["streams"].items() if n != "protocols"]
    if any(t is None or now - t > 1800 for t in entity_times):
        alarms.append("Hay grupos de tokens/pools con más de 30 minutos de retraso o sin tramo confirmado")
    if not continuity_ok:
        alarms.append("Continuidad histórica no verificada; no declarar historias completas")
    if status.get("last_session", {}).get("status") == "FAILED":
        alarms.append("La última sesión informó un fallo")
    if any(
        delta.get(k, 0) for k in ("decode_error", "cycle_read_failure", "market_read_failure", "security_read_failure")
    ):
        alarms.append("Aumentaron errores de lectura o interpretación")
    report = {
        "schema": 1,
        "generated_at": now,
        "day_havana": datetime.fromtimestamp(now, ZONE).date().isoformat(),
        "status": "REQUIERE_REVISION" if alarms else "RECOGIDA_ACTIVA",
        "alarms": alarms,
        "archive_updated_at": status["updated_at"],
        "protocol_covered_until": covered,
        "tokens_observed": status["tokens"],
        "births_observed": status.get("births_observed"),
        "births_old_enough_24h": matured,
        "tokens_with_verified_chain_24h": complete if continuity_ok else None,
        "tokens_economically_evaluable": None,
        "economic_readiness_reason": "Costes, variables y ejecuciones aún no verificados; retornos sin evaluar.",
        "continuity_verified": continuity_ok,
        "gap_details": gaps,
        "ranges_checked_this_audit": ranges_checked,
        "archives_checked_this_audit": len(batches),
        "errors_in_new_archives": dict(errors),
        "quality_counters": quality,
        "quality_increase": delta,
        "source_requests_in_new_archives": dict(requests),
        "external_snapshot_gaps": "No cuantificados por token; un intento de consulta no prueba un dato recibido.",
        "scope": "Contratos y pools listados. 24h de cadena no significa 24h de precios regulares ni venta posible.",
        "financial_execution_enabled": False,
        "outcomes_evaluated": False,
    }
    return report, cursors


def render(report: dict[str, Any]) -> str:
    complete = report["tokens_with_verified_chain_24h"]
    lines = [
        f"BSC · Informe {report['day_havana']} · hora de Cuba",
        f"Estado: {report['status']}.",
        f"Tokens observados: {report['tokens_observed']}; nacimientos capturados: {report['births_observed']}.",
        f"Con 24 horas de cadena verificadas: {complete if complete is not None else 'no verificable'}.",
        f"Ya tienen edad de 24 horas: {report['births_old_enough_24h']}.",
        "Huecos de cadena: "
        + (
            "no detectados en los tramos auditados."
            if report["continuity_verified"]
            else "requieren revisión; ver detalles."
        ),
        "Huecos de precios/otros proveedores: no cuantificados por token; no se suponen completos.",
        "Errores nuevos: " + json.dumps(report["errors_in_new_archives"], ensure_ascii=False) + ".",
        "Cambios de calidad: " + json.dumps(report["quality_increase"], ensure_ascii=False) + ".",
        "Los balances negativos de Transfer son reconstrucciones incompletas, no pérdidas de una billetera.",
        "Aptitud para estrategias: no verificada. No se calcularon ganancias ni se abrió el bloque ciego.",
        "Billetera desconectada. Cero transacciones autorizadas.",
    ]
    if report["alarms"]:
        lines.append("Avisos: " + "; ".join(report["alarms"]) + ".")
    return "\n".join(lines) + "\n"


def put_file(transport: Github, path: str, content: bytes) -> None:
    record = transport.api(f"contents/{path}?ref={REPORT_BRANCH}", missing_ok=True)
    payload: dict[str, Any] = {
        "message": "Record BSC collection health without evaluating outcomes",
        "branch": REPORT_BRANCH,
        "content": base64.b64encode(content).decode(),
    }
    if record:
        payload["sha"] = record["sha"]
    transport.api(f"contents/{path}", payload)


def audit(repo: str, out: Path, *, publish: bool = False) -> dict[str, Any]:
    transport = Github(repo)
    pointer = read_json(transport, HEAD_FILE, BRANCH)
    branch = transport.api(f"git/ref/heads/{REPORT_BRANCH}", missing_ok=True)
    previous = read_json(transport, STATE_FILE, REPORT_BRANCH, missing_ok=True) if branch else None
    if previous is not None and previous.get("schema") != 1:
        raise TransportError("unsupported monitor state")
    baseline = (previous or {}).get("last_archive_sha256")
    current = pointer
    batches = []
    visited: set[str] = set()
    reached = False
    with tempfile.TemporaryDirectory() as temp:
        temp_path = Path(temp)
        for _ in range(MAX_BATCHES):
            if current is None or current["sha256"] == baseline:
                reached = current is not None or baseline is None
                break
            if current["sha256"] in visited:
                raise TransportError("archive ancestry cycle")
            visited.add(current["sha256"])
            path = transport.download(current, temp_path)
            batch = read_archive(path, temp_path)
            batches.append(batch)
            current = batch["manifest"]["parent"]
            path.unlink()
        else:
            reached = (current is None and baseline is None) or (current is not None and current["sha256"] == baseline)
        if not batches:
            # Verify current bytes again, but do not recount already acknowledged ranges/errors.
            path = transport.download(pointer, temp_path)
            batch = read_archive(path, temp_path)
            batch.update(ranges=[], errors={}, source_requests={})
            batches.append(batch)
        batches.reverse()
        report, cursors = assess(batches, previous, reached_baseline=reached, now=time.time())
    report["archive_sha256"] = pointer["sha256"]
    report["archive_tag"] = pointer["tag"]
    report["audit_since"] = (previous or {}).get("last_audit_at")
    day = report["day_havana"]
    daily = (previous or {}).get("daily", {})
    bucket = daily.get(day, {"errors": {}, "quality_increase": {}, "samples": 0, "first_audit_at": time.time()})
    for key, incoming in (
        ("errors", report["errors_in_new_archives"]),
        ("quality_increase", report["quality_increase"]),
    ):
        totals = Counter(bucket[key])
        totals.update(incoming)
        bucket[key] = dict(totals)
    bucket["samples"] += 1
    bucket["last_report"] = report
    daily[day] = bucket
    daily = dict(sorted(daily.items())[-35:])
    state = {
        "schema": 1,
        "last_archive_sha256": pointer["sha256"],
        "last_audit_at": report["generated_at"],
        "coverage_cursors": cursors,
        "continuity_unverified": not report["continuity_verified"],
        "quality": report["quality_counters"],
        "daily": daily,
    }
    daily_report = {
        **report,
        "daily_errors_detected": bucket["errors"],
        "daily_quality_increase": bucket["quality_increase"],
        "daily_audit_samples": bucket["samples"],
        "daily_first_audit_at": bucket["first_audit_at"],
        "day_semantics": "Día de detección; los errores de archivos tardíos pueden pertenecer a eventos anteriores.",
    }
    atomic(out / "latest.json", encoded(report))
    atomic(out / "daily.json", encoded(daily_report))
    atomic(out / "latest.txt", render(report).encode())
    if publish:
        if branch is None:
            info = transport.api("")
            main = transport.api(f"git/ref/heads/{info['default_branch']}")
            transport.api("git/refs", {"ref": "refs/heads/" + REPORT_BRANCH, "sha": main["object"]["sha"]})
        put_file(transport, f"daily/{day}.json", encoded(daily_report))
        put_file(
            transport,
            f"daily/{day}.txt",
            render(
                {**report, "errors_in_new_archives": bucket["errors"], "quality_increase": bucket["quality_increase"]}
            ).encode(),
        )
        put_file(transport, "latest.json", encoded(report))
        put_file(transport, "latest.txt", render(report).encode())
        # Acknowledge last, so a partial publication is recomputed from the previous baseline.
        put_file(transport, STATE_FILE, encoded(state))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    report = audit(args.repo, args.out, publish=args.publish)
    print(render(report))


if __name__ == "__main__":
    main()
