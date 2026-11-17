from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from bsc_lifecycle.network import Reader, ReadError
from bsc_lifecycle.store import Journal, Store
from bsc_lifecycle.wallet_sources import collect, early_url, inspect_activity, parse_buyers

TOKEN = "0x" + "11" * 20
WALLET = "0x" + "22" * 20
OTHER = "0x" + "33" * 20


def test_source_shapes_do_not_turn_profit_fields_into_a_strategy() -> None:
    response = {
        "code": 0,
        "data": {
            "holders": {"holderInfo": [{"wallet_address": WALLET, "status": "sold_part", "realized_profit": 999999}]}
        },
    }
    assert parse_buyers("gmgn_early", response) == [{"wallet": WALLET, "provider_state": "sold_part"}]
    assert parse_buyers("ave_early", {"status": 1, "data": [{"account_address": WALLET, "tag": "early"}]}) == [
        {"wallet": WALLET, "provider_state": "early"}
    ]
    assert parse_buyers("gmgn_early", {"code": 0, "data": {"holders": {}}}) == []


@pytest.mark.parametrize("response", [{"status": 0, "data": []}, {"status": 1}, {"status": 1, "data": [{}]}])
def test_missing_or_restricted_ave_data_is_not_an_empty_history(response: dict[str, Any]) -> None:
    with pytest.raises(ReadError):
        parse_buyers("ave_early", response)


def test_activity_filter_and_pagination_are_not_misrepresented() -> None:
    with pytest.raises(ReadError, match="filter"):
        inspect_activity({"code": 0, "data": {"history": [{"maker": OTHER}]}}, {WALLET})
    result = inspect_activity({"code": 0, "data": {"history": [{"maker": WALLET}], "next": "opaque"}}, {WALLET})
    assert result["more_pages"] is True
    assert result["complete_history"] is False


def test_only_fixed_read_only_hosts_and_valid_addresses() -> None:
    assert early_url("gmgn_early", TOKEN).startswith("https://gmgn.ai/api/v1/tokens/top_buyers/bsc/")
    with pytest.raises(ValueError, match="unsupported"):
        early_url("send_transaction", TOKEN)
    with pytest.raises(ValueError, match="invalid address"):
        early_url("ave_early", "https://attacker.invalid")


def test_poll_retains_first_seen_without_claiming_wallet_skill(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store, journal = Store(tmp_path / "state.sqlite"), Journal(tmp_path)
    store.token(TOKEN, 100)
    store.db.execute("UPDATE tokens SET birth_ts=?,birth_block=100 WHERE address=?", (time.time(), TOKEN))
    store.db.commit()

    async def request(source: str, _url: str) -> Any:
        if source == "gmgn_early":
            return {"code": 0, "data": {"holders": {"holderInfo": [{"wallet_address": WALLET, "status": "hold"}]}}}
        if source == "ave_early":
            return {"status": 1, "data": [{"account_address": WALLET}]}
        return {"code": 0, "data": {"history": [{"maker": WALLET}], "next": None}}

    reader = cast(Reader, type("SourceReader", (), {"request": staticmethod(request)})())
    monkeypatch.setattr("bsc_lifecycle.wallet_sources.asyncio.sleep", AsyncMock())
    result = asyncio.run(collect(store, journal, reader))
    assert all(v["status"] == "OK" for v in result.values())
    first_seen = store.db.execute("SELECT min(first_seen) FROM early_wallets").fetchone()[0]
    store.db.execute("UPDATE wallet_poll SET last_attempt=0")
    store.db.commit()
    asyncio.run(collect(store, journal, reader))
    assert store.db.execute("SELECT count(*) FROM early_wallets").fetchone()[0] == 2
    assert store.db.execute("SELECT min(first_seen) FROM early_wallets").fetchone()[0] == first_seen
    journal.flush()
    records = [json.loads(line) for line in journal.path.read_text().splitlines()]
    assert all(not r["complete_history"] for r in records if r["kind"].endswith("observation"))
    journal.file.close()
    store.db.close()


def test_source_failure_cannot_advance_another_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store, journal = Store(tmp_path / "state.sqlite"), Journal(tmp_path)
    for token in (TOKEN, OTHER):
        store.token(token, 100)
    store.db.execute("UPDATE tokens SET birth_ts=?", (time.time(),))
    store.db.commit()

    async def request(_source: str, _url: str) -> Any:
        raise ReadError("HTTP 403")

    reader = cast(Reader, type("SourceReader", (), {"request": staticmethod(request)})())
    monkeypatch.setattr("bsc_lifecycle.wallet_sources.asyncio.sleep", AsyncMock())
    result = asyncio.run(collect(store, journal, reader))
    assert all(v["status"] == "UNAVAILABLE" for v in result.values())
    assert store.db.execute("SELECT count(*) FROM wallet_poll WHERE token=?", (OTHER,)).fetchone()[0] == 0
    assert store.db.execute("SELECT count(*) FROM early_wallets").fetchone()[0] == 0
    journal.file.close()
    store.db.close()
