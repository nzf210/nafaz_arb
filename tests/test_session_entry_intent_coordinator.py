"""Tests for the V3 two-leg session entry intent coordinator."""
from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from executor.ownership import build_scope_key
from executor.session_entry_intent_coordinator import (
    SessionEntryIntentCoordinator,
    SessionEntryIntentError,
    _index_specs,
    _positive_decimal,
)


def _legs():
    return [
        {
            "leg_id": "spot-1", "leg_type": "SPOT", "status": "CREATED",
            "state_version": 1, "venue": "indodax", "market_type": "spot",
            "symbol": "BTC_IDR", "base_asset": "BTC", "quote_asset": "IDR",
            "settlement_asset": None, "contract_type": None, "contract_size": None,
            "position_side": None, "quantity_unit": "BASE", "base_per_unit": 1,
            "order_quantity": "0.001",
        },
        {
            "leg_id": "future-1", "leg_type": "FUTURES", "status": "CREATED",
            "state_version": 1, "venue": "okx", "market_type": "swap",
            "symbol": "BTC-USDT-SWAP", "base_asset": "BTC", "quote_asset": "USDT",
            "settlement_asset": "USDT", "contract_type": "LINEAR_PERPETUAL",
            "contract_size": "0.001", "position_side": "SHORT",
            "quantity_unit": "CONTRACTS", "base_per_unit": "0.001",
            "order_quantity": "1",
        },
    ]


def _specs():
    return [
        {
            "leg_id": "spot-1", "idempotency_key": "dispatch-1-spot",
            "side": "buy", "order_type": "limit", "quantity": "0.001",
            "price": "900000000", "reserved_base_qty": "0.001",
            "reserved_notional": "900000", "reservation_asset": "IDR",
        },
        {
            "leg_id": "future-1", "idempotency_key": "dispatch-1-future",
            "side": "sell", "order_type": "limit", "quantity": "1",
            "price": "90000", "reserved_base_qty": "0.001",
            "reserved_notional": "90", "reservation_asset": "USDT",
        },
    ]


class _Connection:
    def __init__(self, session, legs):
        self.session = session
        self.legs = legs

    @asynccontextmanager
    async def transaction(self):
        yield self

    async def fetchrow(self, query, *args):
        if "FROM public.execution_sessions" in query:
            return self.session
        if "FROM public.executor_ownership_leases" in query:
            return {"active": True}
        raise AssertionError(f"unexpected fetchrow query: {query}")

    async def fetch(self, query, *args):
        assert "FROM public.execution_legs" in query
        return self.legs


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


class _Intents:
    def __init__(self):
        self.calls = []

    async def prepare_create(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "intent_id": "intent-" + kwargs["leg_id"],
            "session_id": kwargs["session_id"],
            "leg_id": kwargs["leg_id"],
            "venue": kwargs["venue"],
            "instrument": kwargs["instrument"],
            "status": "PREPARED",
        }


class _Repository:
    def __init__(self):
        self.legs = []
        self.sessions = []

    async def transition_leg(self, **kwargs):
        self.legs.append(kwargs)
        return {"aggregate_version": kwargs["expected_aggregate_version"] + 1}

    async def transition_session(self, **kwargs):
        self.sessions.append(kwargs)
        return {"aggregate_version": kwargs["expected_aggregate_version"] + 1}


def _runtime():
    account = "account-1"
    session_id = "session-1"
    owner_id = "coordinator-1"
    scope = build_scope_key(account, "coordinator", "session", session_id)
    session = {
        "session_id": session_id, "status": "ADMITTED", "state_version": 3,
        "aggregate_version": 3, "account_scope_id": account,
        "pair_key": "indodax:btc_idr->okx:btc-usdt-swap",
        "fencing_generation": 4, "coordinator_scope_key": scope,
        "coordinator_owner_id": owner_id,
    }
    lease = SimpleNamespace(
        scope_key=scope, owner_id=owner_id, generation=4,
    )
    conn = _Connection(session, _legs())
    intents = _Intents()
    repository = _Repository()
    service = SessionEntryIntentCoordinator(
        _Pool(conn), order_intents=intents, repository=repository,
    )
    return service, lease, intents, repository


def test_index_specs_requires_exact_one_to_one_leg_mapping():
    legs = _legs()
    with pytest.raises(SessionEntryIntentError, match="one-to-one"):
        _index_specs(legs, [_specs()[0], _specs()[0]])


def test_positive_decimal_rejects_nan_zero_and_negative():
    for value in ("NaN", "Infinity", "0", "-1"):
        with pytest.raises(SessionEntryIntentError):
            _positive_decimal(value, "quantity")


@pytest.mark.asyncio
async def test_prepares_both_intents_before_advancing_session():
    service, lease, intents, repository = _runtime()
    result = await service.prepare_entry(
        session_id="session-1", coordinator_lease=lease,
        specs=_specs(), actor_id="unit-test",
    )
    assert len(intents.calls) == 2
    assert {call["leg_id"] for call in intents.calls} == {"spot-1", "future-1"}
    assert len(repository.legs) == 2
    assert repository.sessions[0]["target_state"] == "ENTRY_IN_PROGRESS"
    assert result["state"] == "ENTRY_IN_PROGRESS"
    assert set(result["intents"]) == {"spot-1", "future-1"}


@pytest.mark.asyncio
async def test_invalid_second_leg_is_rejected_before_any_intent_is_written():
    service, lease, intents, repository = _runtime()
    specs = _specs()
    specs[1]["side"] = "buy"
    with pytest.raises(SessionEntryIntentError, match="FUTURES entry side"):
        await service.prepare_entry(
            session_id="session-1", coordinator_lease=lease,
            specs=specs, actor_id="unit-test",
        )
    assert intents.calls == []
    assert repository.legs == []
    assert repository.sessions == []
