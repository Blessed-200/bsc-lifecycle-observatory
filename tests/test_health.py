from __future__ import annotations

from copy import deepcopy
from typing import Any

from bsc_lifecycle.health import assess, render

NOW = 1794900000.0


def batch() -> dict[str, Any]:
    return {
        "status": {
            "updated_at": NOW - 10,
            "protocol_covered_until": NOW - 30,
            "tokens": 1,
            "births_observed": 1,
            "quality": {"negative_event_balance": 4},
            "last_session": {"status": "RUNNING"},
        },
        "streams": {
            "protocols": {"start": 100, "cursor": 200, "covered_at": NOW - 30},
            "token": {"start": 100, "cursor": 200, "covered_at": NOW - 30},
            "pool": {"start": 150, "cursor": 200, "covered_at": NOW - 30},
        },
        "tokens": [{"address": "token", "birth": NOW - 90000, "birth_block": 100, "stream": "token", "start": 100}],
        "pools": {"token": [{"birth_block": 150, "stream": "pool"}]},
        "ranges": [
            {"stream": "protocols", "from_block": 100, "to_block": 200},
            {"stream": "token", "from_block": 100, "to_block": 200},
            {"stream": "pool", "from_block": 150, "to_block": 200},
        ],
        "errors": {},
        "source_requests": {"rpc-test": 10},
    }


def test_full_chain_coverage_is_not_economic_readiness() -> None:
    report, _ = assess([batch()], None, reached_baseline=True, now=NOW)
    assert report["tokens_with_verified_chain_24h"] == 1
    assert report["tokens_economically_evaluable"] is None
    assert report["outcomes_evaluated"] is False
    assert report["status"] == "RECOGIDA_ACTIVA"
    assert "No se calcularon ganancias" in render(report)


def test_token_only_24h_cannot_hide_missing_pool_history() -> None:
    value = batch()
    value["streams"]["pool"]["covered_at"] = NOW - 86400
    report, _ = assess([value], None, reached_baseline=True, now=NOW)
    assert report["tokens_with_verified_chain_24h"] == 0
    assert report["births_old_enough_24h"] == 1
    assert report["alarms"]


def test_gap_invalidates_complete_claim_even_with_fresh_cursor() -> None:
    value = batch()
    value["ranges"][1]["from_block"] = 105
    report, _ = assess([value], None, reached_baseline=True, now=NOW)
    assert report["gap_details"][0] == {"stream": "token", "from_block": 100, "to_block": 104}
    assert report["tokens_with_verified_chain_24h"] is None
    assert report["status"] == "REQUIERE_REVISION"


def test_restart_and_repeated_audit_do_not_recount_old_errors() -> None:
    value = batch()
    first, cursors = assess([value], None, reached_baseline=True, now=NOW)
    previous = {"coverage_cursors": cursors, "quality": first["quality_counters"]}
    value["ranges"] = []
    repeated, _ = assess([value], previous, reached_baseline=True, now=NOW)
    assert repeated["quality_increase"] == {}
    assert repeated["continuity_verified"] is True
    next_batch = deepcopy(value)
    for stream in next_batch["streams"].values():
        stream["cursor"] = 210
    next_batch["ranges"] = [{"stream": name, "from_block": 201, "to_block": 210} for name in next_batch["streams"]]
    report, _ = assess([next_batch], previous, reached_baseline=True, now=NOW)
    assert report["continuity_verified"] is True


def test_truncated_ancestry_and_existing_gap_stay_unknown() -> None:
    report, _ = assess([batch()], None, reached_baseline=False, now=NOW)
    assert report["continuity_verified"] is False
    report, _ = assess([batch()], {"continuity_unverified": True}, reached_baseline=True, now=NOW)
    assert report["tokens_with_verified_chain_24h"] is None


def test_stale_archive_and_new_failures_raise_alarms_without_reading_returns() -> None:
    value = batch()
    value["status"]["updated_at"] = NOW - 1801
    value["status"]["quality"]["market_read_failure"] = 1
    value["status"]["last_session"]["status"] = "FAILED"
    report, _ = assess([value], None, reached_baseline=True, now=NOW)
    assert len(report["alarms"]) >= 3
    assert report["financial_execution_enabled"] is False
