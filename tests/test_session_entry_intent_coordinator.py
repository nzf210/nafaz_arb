"""Tests for the V3 two-leg session entry intent coordinator."""
from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from executor.ownership import build_scope_key
from executor.session_entry_intent_coordinator import (
    SessionEntryIntentCoordinator,
    SessionEntryIntentError,
    _spot_spec,
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


def test_spot_spec_rejects_futures_entry_specification():
    legs = _legs()
    future_spec = {
        "leg_id": "future-1", "idempotency_key": "dispatch-1-future",
        "side": "sell", "order_type": "limit", "quantity": "1", "price": "90000",
    }
    with pytest.raises(SessionEntryIntentError, match="SPOT leg only"):
        _spot_spec(legs, [future_spec])


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


@pytest.mark.asyncio
async def test_duplicate_leg_idempotency_keys_are_rejected_before_writes():
    service, lease, intents, repository = _runtime()
    specs = _specs()
    specs[1]["idempotency_key"] = specs[0]["idempotency_key"]
    with pytest.raises(SessionEntryIntentError, match="distinct idempotency key"):
        await service.prepare_entry(
            session_id="session-1", coordinator_lease=lease,
            specs=specs, actor_id="unit-test",
        )
    assert intents.calls == []
    assert repository.legs == []
    assert repository.sessions == []


@pytest.fixture
async def postgres_pool():
    import os
    dsn = os.getenv("TEST_POSTGRES_DSN", "").strip()
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")
    import asyncpg
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=8)
    try:
        yield pool
    finally:
        await pool.close()


def _integration_policy_snapshots():
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    evidence = {
        name: {
            "status": "VERIFIED" if name != "fx_conversion" else "NOT_REQUIRED",
            "observed_at": now.isoformat(),
            "valid_until": (now + timedelta(minutes=2)).isoformat(),
        }
        for name in ("market_metadata", "fee_schedule", "fx_conversion")
    }
    return {
        "policy_snapshot": {"strategy": "spot-perp", "version": 3, "evidence": evidence},
        "execution_policy_snapshot": {"mode": "maker-taker"},
        "risk_policy_snapshot": {"max_unhedged_ms": 3000},
        "exit_policy_snapshot": {"take_profit": {"net_roi_pct": 0.3}},
    }


@pytest.mark.asyncio
async def test_gateway_advances_session_leg_to_submitting_before_venue_call(postgres_pool):
    import uuid
    from executor.session_coordinator import SessionCoordinator
    from executor.ownership import OwnershipLeaseService
    from executor.order_intent_service import OrderIntentService
    from executor.global_stop import GlobalStopService
    from executor.mutation_gateway import MutationGateway, MutationResultType

    suffix = uuid.uuid4().hex
    account = "entry-submit-account-" + suffix
    session_legs = [
        {
            "leg_id": "spot-" + suffix, "leg_type": "SPOT", "venue": "indodax",
            "market_type": "spot", "symbol": "btc_idr", "normalized_symbol": "BTC/IDR",
            "base_asset": "BTC", "quote_asset": "IDR", "quantity_unit": "BASE",
            "base_per_unit": "1", "order_quantity": "0.001",
            "exposure_quantity": "0.001", "target_qty": "0.001",
        },
        {
            "leg_id": "future-" + suffix, "leg_type": "FUTURES", "venue": "okx",
            "market_type": "swap", "symbol": "BTC-USDT-SWAP",
            "normalized_symbol": "BTC/USDT:USDT", "base_asset": "BTC",
            "quote_asset": "USDT", "settlement_asset": "USDT",
            "contract_type": "LINEAR_PERPETUAL", "contract_size": "0.001",
            "position_side": "SHORT", "quantity_unit": "CONTRACTS",
            "base_per_unit": "0.001", "order_quantity": "1",
            "exposure_quantity": "0.001", "target_qty": "1",
        },
    ]
    coordinator = SessionCoordinator(
        pool=postgres_pool,
        ownership=OwnershipLeaseService(owner_id="entry-submit-coordinator-" + suffix),
    )
    admitted = await coordinator.admit(
        opportunity_id="opp-" + suffix, source_dispatch_id="dispatch-" + suffix,
        account_scope_id=account,
        pair_key="indodax:btc_idr->okx:btc-usdt-swap",
        legs=session_legs, actor_id="integration-test",
        **_integration_policy_snapshots(),
    )
    lease = admitted["coordinator_lease"]
    for state, state_version, aggregate in (
        ("VALIDATING", 1, 3), ("ADMITTED", 2, 4),
    ):
        await coordinator.transition(
            coordinator_lease=lease, session_id=admitted["session_id"],
            target_state=state, expected_state_version=state_version,
            expected_aggregate_version=aggregate,
            fencing_generation=lease.generation,
            event_type="SESSION_" + state,
            event_idempotency_key=admitted["admission_key"] + ":" + state.lower(),
            actor_id="integration-test",
        )

    intents = OrderIntentService(postgres_pool)
    entry = SessionEntryIntentCoordinator(postgres_pool, order_intents=intents)
    risk_metrics = {
        "gross_exposure_usdt": 0, "net_exposure_usdt": 0,
        "deployed_capital_usdt": 0, "unhedged_notional_usdt": 0,
        "unhedged_duration_ms": 0, "open_order_count": 0,
        "daily_loss_usdt": 0, "drawdown_pct": 0,
        "slippage_bps": 0, "funding_cost_pct": 0,
        "market_data_age_ms": 0, "venue_health_score": 1,
    }
    prepared = await entry.prepare_entry(
        session_id=admitted["session_id"], coordinator_lease=lease,
        actor_id="integration-test",
        specs=[
            {
                "leg_id": "spot-" + suffix, "idempotency_key": "entry-spot-" + suffix,
                "side": "buy", "order_type": "limit", "quantity": "0.001",
                "price": "900000000", "reserved_base_qty": "0.001",
                "reserved_notional": "900000", "reservation_asset": "IDR",
                "request_metadata": {"action_class": "ENTRY"},
            },
            {
                "leg_id": "future-" + suffix, "idempotency_key": "entry-future-" + suffix,
                "side": "sell", "order_type": "limit", "quantity": "1",
                "price": "90000", "reserved_base_qty": "0.001",
                "reserved_notional": "90", "reservation_asset": "USDT",
                "request_metadata": {"action_class": "ENTRY", "risk_metrics": risk_metrics},
            },
        ],
    )
    global_stop = GlobalStopService("unused", require_scoped_submits=True)
    global_stop.pool = postgres_pool
    class Venue:
        id = "okx"
        calls = []
        async def create_order_direct(self, **kwargs):
            self.calls.append(kwargs)
            return {"id": "venue-order-" + suffix, "status": "open", "filled": "0"}
    venue = Venue()
    gateway = MutationGateway(global_stop=global_stop, order_intent_service=intents)
    result = await gateway.execute_create_intent(
        venue, intent_id=prepared["intents"]["future-" + suffix]["intent_id"],
        actor_id="integration-test",
    )
    assert result.status == MutationResultType.CONFIRMED
    assert len(venue.calls) == 1
    async with postgres_pool.acquire() as conn:
        leg = await conn.fetchrow(
            "SELECT status, state_version FROM public.execution_legs WHERE session_id=$1 AND leg_id=$2",
            admitted["session_id"], "future-" + suffix,
        )
        event = await conn.fetchrow(
            """
            SELECT event_type, payload FROM public.execution_session_events
            WHERE session_id=$1 AND event_idempotency_key=$2
            """,
            admitted["session_id"], "mutation-submit:" + prepared["intents"]["future-" + suffix]["intent_id"],
        )
        assert leg["status"] == "SUBMITTING"
        assert event["event_type"] == "LEG_SUBMISSION_STARTED"
        await global_stop.ownership.release_all(conn)
    await coordinator.release(lease)
